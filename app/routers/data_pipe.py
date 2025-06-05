from fastapi import APIRouter
import pandas as pd

# Local imports
from dataprobe import fake_data, _stat_column_data
from routers.schema import FakeDataRequest, StatColumnDataRequest


router = APIRouter()


@router.post("/fake_data")
async def fake_data_api(request: FakeDataRequest):
    try:
        data = pd.Series(request.data)
        result = fake_data(data, request.n_samples)
        return {'status': 'success', 'result': result.tolist()}
    except Exception as e:
        return {'status': 'error', 'message': f'Failed to generate fake data: {e}'}


@router.post("/stat_column_data")
async def stat_column_data_api(request: StatColumnDataRequest):
    try:
        data = pd.Series(request.data)
        result = _stat_column_data(data)
        keep_fields = [
            'determined_type', 'total_count', 'na_count', 'na_percentage', 'valid_count'
        ]
        result = {k: result[k] for k in keep_fields}
        return {'status': 'success', 'result': result}
    except Exception as e:
        return {'status': 'error', 'message': f'Failed to stat column data: {e}'}