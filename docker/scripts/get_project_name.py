import tomllib

with open("/app/pyproject.toml", "rb") as f:
    data = tomllib.load(f)

print(data["project"]["name"], end="")
