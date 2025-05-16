import os
import hashlib
import polars as pl
import glob


PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)


def create_hash_key(obj) -> str:
    """
    dataclass 인스턴스의 모든 필드를 str로 변환, trim, join하여 blake2b 해시로 unique key를 생성합니다.
    """
    # 모든 필드 값을 str로 변환 후 strip
    values = [str(getattr(obj, field)).strip() for field in obj.__dataclass_fields__]
    joined = "|".join(values)
    # blake2b는 빠르고 충돌 가능성도 낮음
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
