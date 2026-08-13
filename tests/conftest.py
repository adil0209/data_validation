import pytest

from data_validation.spark_session import get_spark_session


@pytest.fixture(scope="session")
def spark():
    session = get_spark_session(app_name="data-validation-tests", enable_azure=False)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
