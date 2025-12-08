import requests
from core.config import DATALARK_URL, DATALARK_TOKEN


def send_payload(payload: dict) -> tuple[bool, str]:
    """
    Send failures to Data Lark API.

    Returns
    -------
    tuple[bool, str]
        (success, message) - success status and error/success message
    """
    try:
        headers = {
            "Authorization": f"Bearer {DATALARK_TOKEN}",
            "Content-Type": "application/json"
        }
        resp = requests.post(DATALARK_URL, headers=headers, json=payload, timeout=30)

        if resp.status_code == 200:
            return True, "Success"
        else:
            # Try to get error details from response
            try:
                error_detail = resp.json()
            except Exception:
                error_detail = resp.text[:200] if resp.text else "No response body"

            return False, f"Status {resp.status_code}: {error_detail}"

    except requests.exceptions.Timeout:
        return False, "Request timed out after 30 seconds"
    except requests.exceptions.ConnectionError as e:
        return False, f"Connection error: {e}"
    except Exception as e:
        return False, f"Unexpected error: {e}"
