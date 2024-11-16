import json

class ZenConnection:

    BASE_URL = 'https://api.zenmoney.ru/v8/diff/'
    FILE_NAME = 'zenmoney.json'

    def __init__(self, token:str, last_sync_timestamp:int=0) -> None:
        self._token = token
        self._sync_timestamp = last_sync_timestamp

    @property
    def sync_timestamp(self) -> int:
        return self._sync_timestamp

    @sync_timestamp.setter
    def sync_timestamp(self, server_timestamp:int) -> None:
        self._sync_timestamp = server_timestamp

    def write_zfile(self, z_json) -> None:
        import os
        if not os.path.exists(self.FILE_NAME):
            with open(self.FILE_NAME, 'w') as f:
                f.write(json.dumps(z_json))
        else:
            print(f"File exists: {self.FILE_NAME}")

    def sync(self, diff=None):
        import time
        import requests

        headers = {
            'Authorization': f'Bearer {self._token}',
            'Content-Type': 'application/json',
        }

        json_data = {
            'serverTimestamp': self.sync_timestamp,
            'currentClientTimestamp': int(time.time()),
        }

        if diff:
            json_data.update(diff)

        response = requests.post(self.BASE_URL, headers=headers, json=json_data)
        if response.status_code == 400:
            print(response.json())
        response.raise_for_status()
        resp = response.json()
        self.sync_timestamp = resp['serverTimestamp']
        return resp

class Zenmoney:

    def __init__(self, zdict):
        self._zdict = zdict

    @classmethod
    def load(cls, filename):
        with open(filename) as f:
            zdict = json.load(f)
        return cls(zdict)

    def apply_diff(self, diff: dict) -> None:
        for field_name in ['instrument', 'company', 'user', 'account',
                           'tag', 'merchant', 'budget', 'reminder', 'reminderMarker', 'transaction']:
            if field_name in diff:
                self._zdict[field_name] = self._zdict.get(field_name, []) + diff[field_name]
                print(f"Updated {field_name}: {len(diff[field_name])} item(s)")

            if 'deletion' in diff:
                for del_item in diff['deletion']:
                    try:
                        self._zdict[del_item['object']]\
                            = [x for x in self._zdict[del_item['object']] if x['id'] != self._zdict[del_item['id']]]
                        print(f"Removed {del_item['object']}")
                    except KeyError:
                        print(f"Remove {del_item['object']} not found")

            self._zdict['serverTimestamp'] = diff['serverTimestamp']

    def write(self, filename):
        with open(filename, 'w') as f:
            f.write(json.dumps(self._zdict))


    def get_by_value(self, prop, field, value):
        return [x for x in self._zdict[prop] if x[field] == value]


    @property
    def server_timestamp(self) -> int:
        if not 'serverTimestamp' in self._zdict:
            raise ValueError('serverTimestamp not in dict!')
        return self._zdict['serverTimestamp']

    @property
    def instrument(self) -> list:
        return self._zdict.get('instrument', [])

    @property
    def country(self) -> list:
        return self._zdict.get('country', [])

    @property
    def company(self) -> list:
        return self._zdict.get('company', [])

    @property
    def user(self) -> list:
        return self._zdict.get('user', [])

    @property
    def account(self) -> list:
        return self._zdict.get('account', [])

    @property
    def tag(self) -> list:
        return self._zdict.get('tag', [])

    @property
    def budget(self) -> list:
        return self._zdict.get('budget', [])

    @property
    def merchant(self) -> list:
        return self._zdict.get('merchant', [])

    @property
    def reminder(self) -> list:
        return self._zdict.get('reminder', [])

    @property
    def reminder_marker(self) -> list:
        return self._zdict.get('reminder_marker', [])

    @property
    def transaction(self) -> list:
        return self._zdict.get('transaction', [])
