def get_random_number_from_api(min: int, max: int, count: int) -> int:
    import requests

    r = requests.get(
        f"http://www.randomnumberapi.com/api/v1.0/random?min={min}&max={max}&count={count}"
    )

    return r.json()[0]