#!/usr/local/bin/python2.7

# Usage in cron file (assumes the script is installed in /root/listenagain and
# is executable, and that Python 2.7 is available):
# * * * * * /root/listenagain/la_update_recent.py ~listenrs/nowplaying/main/onair.txt ~listenrs/nowplaying
# * * * * * sleep 30s && /root/listenagain/la_update_recent.py ~listenrs/nowplaying/main/onair.txt ~listenrs/nowplaying


MAX_HISTORY = 50
HISTORY_USER = 'listenrs'
HISTORY_GROUP = 'listenrs'
ONAIR_ENCODING = 'iso-8859-1'
NOW = __import__('time').time()
DEFAULT_DURATION_IF_NONE_SUPPLIED = (3 * 60) + 30

RECENT_FNAME = 'recent.json'
HISTORY_SUBDIR = 'all'

def parse_onair_file(fname):
    import codecs
    import os
    import re
    import sys
    pattern = re.compile("^(?P<a>^.*?) - (?P<t>.*?)(?: \(Genre: (?P<g>.+?) - Duration: (?P<m>[0-9]+):(?P<s>[0-9]+)\).*)?$")
    try:
        f = codecs.open(fname, 'r', encoding=ONAIR_ENCODING)
    except (IOError, OSError):
        sys.stderr.write('Failed to open %s for reading.\n' % (fname,))
        return None
    text = f.read()
    f.close()
    match = pattern.match(text)
    if match is None:
        return None
    match_dict = match.groupdict()
    data = {
        'artist': match_dict['a'],
        'title': match_dict['t'],
    }
    genre = match_dict['g']
    if genre is None:
        genre = ''
    data['genre'] = genre
    minutes = match_dict['m']
    seconds = match_dict['s']
    if minutes is None and seconds is None:
        data['duration'] = DEFAULT_DURATION_IF_NONE_SUPPLIED
    else:
        if minutes is None:
            minutes = 0
        else:
            minutes = int(minutes)
        if seconds is None:
            seconds = 0
        else:
            seconds = int(seconds)
        data['duration'] = (60 * minutes) + int(seconds)
    try:
        fstat = os.stat(fname)
    except (IOError, OSError):
        sys.stderr.write('Failed to get stats for %s.\n' % (fname,))
    else:
        data['start'] = fstat.st_mtime
        data['last_change'] = fstat.st_mtime

    return data


def has_onair_file_changed(onair_fname, history_fname):
    import json
    import os
    import sys

    try:
        fstat = os.stat(onair_fname)
    except (IOError, OSError):
        sys.stderr.write('Failed to get stats for %s.\n' % (onair_fname,))
        return False
    onair_mtime = fstat.st_mtime

    try:
        hf = open(history_fname, 'r')
    except (IOError, OSError):
        sys.stderr.write('Failed to open %s for reading.\n' % (history_fname,))
        return True

    try:
        history_data = json.load(hf)
    except ValueError, e:
        sys.stderr.write('Error parsing json file %s:\n%s\n' % (history_fname, e))
        hf.close()
        return True
    hf.close()

    if 'last_change' not in history_data:
        return True
    last_change = history_data['last_change']
    if last_change < onair_mtime:
        return True
    if history_data.get('is_playing', False) \
    and history_data.get('current_duration', 0) and history_data['expect_end'] <= NOW:
        return True
    return False


def _enforce_ownership(fname, user_name, group_name):
    import grp
    import os
    import pwd
    import sys

    try:
        urecord = pwd.getpwnam(user_name)
    except KeyError:
        sys.stderr.write('Could not get uid for user %s.\n' % (user_name,))
        uid = -1
    else:
        uid = urecord.pw_uid
    try:
        grecord = grp.getgrnam(group_name)
    except KeyError:
        sys.stderr.write('Could not get gid for group %s.\n' % (group_name,))
        gid = -1
    else:
        gid = grecord.gr_gid
    os.chown(fname, uid, gid)


def update_history_file(onair_data, history_fname, all_history_dir):
    import json
    import os
    import sys
    import time

    try:
        hf = open(history_fname, 'a+')
    except (IOError, OSError):
        sys.stderr.write('Failed to open %s in "a+" mode.\n' % (history_fname,))
        return None

    try:
        history_data = json.load(hf)
    except ValueError, e:
        sys.stderr.write('Error parsing json file %s (discarding data):\n%s\n' % (history_fname, e))
        history_data = {}

    changed = False
    expect_end = onair_data['last_change'] + onair_data['duration']
    if history_data.get('last_change', 'invalid!') != onair_data['last_change']:
        history_data['last_change'] = onair_data['last_change']
        history_data['current_duration'] = onair_data['duration']
        history_data['expect_end'] = expect_end

        history_list = history_data.setdefault('history', [])
        history_list.append({
            'artist': onair_data['artist'],
            'title': onair_data['title'],
            'genre': onair_data['genre'],
            'duration': onair_data['duration'],
            'start': onair_data['start'],
        })
        if len(history_list) > MAX_HISTORY:
            del history_list[:-MAX_HISTORY]
        changed = True

    if history_data.get('current_duration', 0) \
    and 'expect_end' in history_data and history_data['expect_end'] > NOW:
        is_playing = True
    else:
        is_playing = False
    if is_playing != history_data.get('is_playing', None):
        history_data['is_playing'] = is_playing
        changed = True

    if changed:
        history_data['last_history_change'] = NOW
        hf.seek(0)
        hf.truncate()
        json.dump(history_data, hf, sort_keys=True)
    hf.close()
    _enforce_ownership(history_fname, HISTORY_USER, HISTORY_GROUP)

    history_list = history_data.get('history', [])

    if changed and is_playing and history_list:
        year, month, day = time.gmtime(NOW)[:3]
        dated_fname = os.path.join(
            all_history_dir,
            '%4.4d' % (year,),
            '%2.2d' % (month,),
            '%2.2d.json' % (day,),
        )
        dated_fname_dir = os.path.split(dated_fname)[0]
        if not os.path.exists(dated_fname_dir):
            os.makedirs(dated_fname_dir)

        try:
            dhf = open(dated_fname, 'a+')
        except (IOError, OSError):
            sys.stderr.write('Failed to open %s in "a+" mode.\n' % (dated_fname,))
        else:

            try:
                dated_history_data = json.load(dhf)
            except ValueError, e:
                sys.stderr.write('Error parsing json file %s (discarding data):\n%s\n' % (dated_fname, e))
                dated_history_data = {
                    'year': year,
                    'month': month,
                    'day': day,
                }
            dated_history_data.setdefault('history', []).append(history_list[-1])
            dated_history_data['last_history_change'] = NOW

            dhf.seek(0)
            dhf.truncate()
            json.dump(dated_history_data, dhf, sort_keys=True)
            dhf.close()
            _enforce_ownership(dated_fname, HISTORY_USER, HISTORY_GROUP)

    return len(history_data.get('history', []))


def main(onair_fname, history_dir):
    import os
    history_fname = os.path.join(history_dir, RECENT_FNAME)
    all_history_dir = os.path.join(history_dir, HISTORY_SUBDIR)
    if has_onair_file_changed(onair_fname, history_fname):
        onair_data = parse_onair_file(onair_fname)
        if onair_data is not None:
            result = update_history_file(onair_data, history_fname, all_history_dir)
            if result is not None:
                return 0
        return 1
    return 0 # no change to onair data


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 3:
        print 'Usage: %s ONAIR_FILE HISTORY_DIR' % (sys.argv[0],)
        sys.exit(0)

    onair_fname = sys.argv[1]
    history_dir = sys.argv[2]

    return_code = main(onair_fname, history_dir)
    sys.exit(return_code)
