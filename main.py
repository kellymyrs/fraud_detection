from datetime import datetime, timedelta, date
import json
from faker import Faker
import random
from random import randint
import os.path


def print_hi(name):
    request = {'request_id': randint(0, 100), 'request_timestamp': datetime.now().timestamp(), 'longtitude': random.uniform(-180.0, 180.0), 'latitude': random.uniform(-90.0, 90.0), 'document_photo_brightness_percent': randint(0, 100), 'is_photo_in_a_photo_selfie': randint(0, 100)}
    path = os.path.join('./request', 'year=' + str(datetime.now().year), 'month=' + str(datetime.now().month), 'hour=' + str(datetime.now().hour) + '/')
    os.makedirs(path)
    filename = path + "request.json"
    with open(filename, "w") as outfile:
        json.dump(request, outfile)
    print(request)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
