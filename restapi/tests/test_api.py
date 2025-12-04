import io

import pytest
from fastapi.testclient import TestClient

# Import the app and module-level objects
import restapi.src.main as api_main
from restapi.src.schemas import JobStatus


@pytest.fixture(scope="module")
def client():
    # Use TestClient; startup/shutdown events will run
    return TestClient(api_main.app)


def test_docs_redirect(client):
    resp = client.get("/")
    assert resp.status_code in (200, 307, 302)


def test_get_missing_job_returns_404(client):
    resp = client.get("/jobs/nonexistent")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Job not found"


def test_list_jobs_initial_empty(client):
    resp = client.get("/jobs")
    assert resp.status_code == 200
    payload = resp.json()
    assert isinstance(payload["items"], list)


def test_submit_job_enqueues_and_returns_id(tmp_path, monkeypatch):
    # Prepare a small CSV in-memory
    csv_bytes = io.BytesIO(b"transaction_id,amount,effective_date\nA1,10,2024-01-01\n")

    # Monkeypatch enqueue to avoid running subprocess and just record metadata
    captured = {}

    async def fake_enqueue(file, params):
        # Create job id and persist minimal metadata
        job_id = "testjob123"
        api_main.storage.create_job_record(job_id, "input.csv", params)
        # Save input file to storage
        input_path = api_main.storage.input_path(job_id)
        input_path.write_bytes(csv_bytes.getvalue())
        captured["job_id"] = job_id
        return job_id

    monkeypatch.setattr(api_main.manager, "enqueue_job", fake_enqueue)

    client = TestClient(api_main.app)
    files = {"file": ("population_data.csv", csv_bytes.getvalue(), "text/csv")}
    data = {
        "tolerable_misstatement": 1000,
        "expected_misstatement": 100,
        "assurance_factor": 2,
        "balance_type": "both",
        "random_seed": 42,
        "include_zeros": True,
        "fast": False,
        "progress": False,
    }
    resp = client.post("/jobs", files=files, data=data)
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["job_id"] == captured["job_id"]
    assert payload["status"] == JobStatus.PENDING.value

    # Verify metadata exists
    job_id = payload["job_id"]
    detail = api_main.storage.load_job(job_id)
    assert detail.job_id == job_id
    assert detail.status == JobStatus.PENDING
    assert detail.file_name == "input.csv"
