import os
import hashlib
import polars as pl
import glob


PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)


def create_hash_key(obj, is_dataclass: bool = True) -> str:
    """
    dataclass 인스턴스, dict, list/tuple 모두 지원하여 unique key를 생성합니다.
    
    dataclass의 경우 모든 필드로 unique key를 생성합니다.
    그 외의 경우 추출된 필드로만 생성합니다.
    """
    if is_dataclass:
        values = [str(getattr(obj, field)).strip() for field in obj.__dataclass_fields__]
    elif hasattr(obj, 'values'):  # dict
        values = [str(v).strip() for v in obj.values()]
    else:  # list, tuple 등
        values = [str(v).strip() for v in obj]
    joined = "|".join(values)
    hash_key = hashlib.blake2b(joined.encode("utf-8"), digest_size=16).hexdigest()
    return hash_key


def create_yyyymm_form_date_list(year: int) -> list:
    """
    주어진 년도에 대해 1월부터 12월까지의 문자열 형식의 날짜 리스트를 생성합니다.

    Args:
        year (int): 년도
    """
    return [f"{year}{month:02d}" for month in range(1, 13)]


def save_file_to_local(
    data: list[dict], file_name: str, file_type: str = "parquet"
) -> None:
    # 프로젝트 홈(최상위) 기준으로 tmp 폴더 생성
    tmp_dir = os.path.join(PROJECT_ROOT, "tmp")
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    file_name = f"{file_name}.{file_type}"
    file_path = os.path.join(tmp_dir, file_name)

    if os.path.exists(file_path):
        os.remove(file_path)

    df = pl.DataFrame(data)
    df.write_parquet(file_path)
    print(f"Saved {file_name} to {tmp_dir}")


def read_file_path(file_name: str, file_type: str = "parquet"):
    tmp_dir = os.path.join(PROJECT_ROOT, "tmp")

    file_path = os.path.join(tmp_dir, f"{file_name}.{file_type}")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    return file_path


def remove_file_from_local():
    tmp_dir = os.path.join(PROJECT_ROOT, "tmp")
    if os.path.exists(tmp_dir):
        for file_path in glob.glob(os.path.join(tmp_dir, "*")):
            if os.path.isfile(file_path):
                os.remove(file_path)
        print(f"Removed all files in {tmp_dir}")


def chunk_list(lst: list, chunk_size: int):
    """리스트를 n개씩 chunk로 나누는 유틸 함수"""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]