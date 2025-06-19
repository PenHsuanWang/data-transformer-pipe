import uuid
def uuid4() -> str:  # one-liner helper
    return uuid.uuid4().hex
