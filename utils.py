import os
import pause

API_RETRY_COUNT = int(os.getenv("API_RETRY_COUNT"))
API_RETRY_WAIT_MINS = int(os.getenv("API_RETRY_WAIT_MINS"))
DEBUG = bool(int(os.getenv("DEBUG_SCRIPT")))

def retry_query(func):
    def wrapper():
        current_retries = 0
        retry_success = False
        while (current_retries < API_RETRY_COUNT) and (not retry_success):
            try:
                res = func()
                retry_success = True
                return res
            except:
                print("Failed, trying again")
                current_retries += 1
                if DEBUG:
                    pause.seconds(API_RETRY_WAIT_MINS)
                else:
                    pause.minutes(API_RETRY_WAIT_MINS)

        if not retry_success:
            raise ValueError("Could not make query")
    return wrapper