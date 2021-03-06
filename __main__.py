from bitdeli import Profiles, set_theme, Title, Description
from bitdeli.textutil import Percent
from itertools import chain, groupby
from datetime import datetime
import time

WINDOW = 72 #hours
NUM_WEEKS = 4
ACTIVE_LIMIT = 2 # hours
TARGET_HOURS = 10

text = {}

set_theme('purple')

def is_success(active_hours):
    return len(active_hours) > TARGET_HOURS

def active_users(profiles):
    for profile in profiles:
        all_hours = chain.from_iterable(profile['events'].itervalues())
        active_hours = frozenset(hour for hour, count in all_hours)
        if len(active_hours) > ACTIVE_LIMIT:
            yield min(active_hours),\
                  max(active_hours),\
                  is_success(active_hours),\
                  profile.uid

def newest_active(active_profiles):
    def format(now, hour):
        t = datetime.utcfromtimestamp(hour * 3600).strftime('%Y-%m-%d %H:00')
        return '%d hours ago (%s)' % (now - hour, t)

    def window():
        now = time.time() / 3600
        newest_profile = None
        for first_hour, last_hour, success, uid in sorted(active_profiles,
                                                          reverse=True):
            if newest_profile == None:
                newest_profile = first_hour
            if newest_profile - first_hour > WINDOW:
                break
            yield {' id': uid,
                   'first event': format(now, first_hour),
                   'last event': format(now, last_hour)}
    data = list(window())
    text['num_active'] = len(data)
    yield {'type': 'table',
           'label': 'newest active users (%d)' % len(data),
           'size': (12, 5),
           'data': data}

def progress(active_profiles):
    def weeks():
        this_week = None
        for first_hour, last_hour, success, uid in sorted(active_profiles,
                                                          reverse=True):
            week = datetime.utcfromtimestamp(first_hour * 3600).isocalendar()[1]
            if this_week == None:
                this_week = week
            if this_week - week > NUM_WEEKS:
                break
            yield week, success

    def weekly_scores():
        for week, successes in groupby(weeks(), lambda x: x[0]):
            total = num_success = 0
            data = list(successes)
            num_success = sum(1 for day, success in data if success)
            ratio = num_success / float(len(data))
            if 'num_converted' not in text:
                text['num_converted'] = num_success
                text['conversion'] = Percent(ratio)
            yield 'week %d' % week, ratio * 100
            
    yield {'type': 'bar',
           'color': 3,
           'label': '% of users converted',
           'data': list(reversed(list(weekly_scores()))),
           'size': (12, 3)}

Profiles().map(active_users).map(newest_active).show()
Profiles().map(active_users).map(progress).show()

Title('{num_active} new users are waiting to be converted!', text)

Description("""
Already {num_converted} users have become active this week. Our conversion percent is currently {conversion}.
""", text)