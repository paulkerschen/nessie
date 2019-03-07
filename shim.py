import time

from nessie.factory import create_app
app = create_app()
ac = app.app_context()
ac.push()

from nessie.lib.queries import get_advisee_student_profile_feeds
t = time.time()
print('Generating advisee IDs map')
advisee_ids_map = {}
all_student_feeds = get_advisee_student_profile_feeds()
for index, student_feed in enumerate(all_student_feeds):
    sid = student_feed.get('sid')
    canvas_user_id = student_feed.get('canvas_user_id')
    uid = student_feed.get('ldap_uid')
    if canvas_user_id and uid:
        advisee_ids_map[canvas_user_id] = {'sid': sid, 'uid': uid}
print(f'{time.time() - t} seconds')

from nessie.merged.student_terms import get_sis_enrollments, merge_dropped_classes, merge_term_gpas
t = time.time()
print('Getting SIS enrollments')
all_advisees_terms_map = get_sis_enrollments()
print('Merging dropped classes')
merge_dropped_classes(all_advisees_terms_map)
print('Merging term GPAs')
merge_term_gpas(all_advisees_terms_map)
print(f'{time.time() - t} seconds')

from nessie.merged.student_terms import get_canvas_site_map, merge_memberships_into_site_map, merge_canvas_data
t = time.time()
print('Getting Canvas site map')
(canvas_site_map, advisee_site_map) = get_canvas_site_map()
print('Merging memberships into site map')
merge_memberships_into_site_map(canvas_site_map)
print('Merging Canvas data')
merge_canvas_data(canvas_site_map, advisee_site_map, all_advisees_terms_map)
print(f'{time.time() - t} seconds')


from itertools import groupby
import operator

from nessie.lib.analytics import mean_assignment_submissions_for_user, mean_analytics_except_assignment_submissions_for_user


print('---MERGING ANALYTICS DATA---')

def merge_advisee_assignment_submissions(terms_map, canvas_user_id, relative_submission_counts, canvas_site_map):
    for (term_id, term_feed) in terms_map.items():
        canvas_courses = []
        for enrollment in term_feed.get('enrollments', []):
            canvas_courses += enrollment['canvasSites']
        canvas_courses += term_feed.get('unmatchedCanvasSites', [])
        # Decorate the Canvas courses list with per-course statistics and return summary statistics.
        term_feed['analytics'] = term_feed['analytics'] or {}
        term_feed['analytics']['assignmentSubmissions'] = mean_assignment_submissions_for_user(
            canvas_courses,
            canvas_user_id,
            relative_submission_counts,
            canvas_site_map,
        )
    return terms_map


def merge_advisee_analytics_except_assignment_submissions(terms_map, canvas_user_id, relative_submission_counts, canvas_site_map):
    for (term_id, term_feed) in terms_map.items():
        canvas_courses = []
        for enrollment in term_feed.get('enrollments', []):
            canvas_courses += enrollment['canvasSites']
        canvas_courses += term_feed.get('unmatchedCanvasSites', [])
        # Decorate the Canvas courses list with per-course statistics and return summary statistics.
        term_feed['analytics'] = term_feed['analytics'] or {}
        term_feed['analytics'].merge(mean_analytics_except_assignment_submissions_for_user(
            canvas_courses,
            canvas_user_id,
            relative_submission_counts,
            canvas_site_map,
        ))
    return terms_map

# First, handle those advisees who are graced with billions and billions of assignment submission stats.
# Track analytics already merged in the event a retry is needed on the submission stats query.
merged_analytics = {}

def submissions_generator():
    submissions_data = open('/Users/paulkerschen/tmp/sub_000')
    columns = ['reference_user_id', 'canvas_course_id', 'canvas_user_id', 'submissions_turned_in']
    for line in submissions_data:
        yield dict(zip(columns, [(int(f) if f.isdigit() else None) for f in line.strip().split(',')]))

all_counts_query = submissions_generator()

t = time.time()
user_count = 0

advisee_ids = list(advisee_ids_map.keys())

print(f'Starting non-assignment-submissions analytics merge for {len(advisee_ids)} advisees')
for canvas_user_id in advisee_ids:
    print(f'Here is Canvas ID {canvas_user_id} (user {user_count}, {time.time() - t} seconds)')
    sid = advisee_ids_map[canvas_user_id].get('sid')
    advisee_terms_map = all_advisees_terms_map.get(sid)
    if not advisee_terms_map:
        # Nothing to merge.
        continue
    merge_advisee_analytics(advisee_terms_map, canvas_user_id, {}, canvas_site_map)

t = time.time()
user_count = 0

print(f'Starting assignment-submissions analytics merge for advisees with assignment stats')
for canvas_user_id, sites_grp in groupby(all_counts_query, key=operator.itemgetter('reference_user_id')):
    user_count += 1
    print(f'Here is Canvas ID {canvas_user_id} (user {user_count}, {time.time() - t} seconds)')
    if merged_analytics.get(canvas_user_id):
        # We must have already handled calculations for this user on a download that subsequently errored out.
        continue
    sid = advisee_ids_map.get(canvas_user_id, {}).get('sid')
    if not sid:
        print(f'Advisee submissions query returned canvas_user_id {canvas_user_id}, but no match in advisees map')
        merged_analytics[canvas_user_id] = 'skipped'
        continue
    advisee_terms_map = all_advisees_terms_map.get(sid)
    if not advisee_terms_map:
        # Nothing to merge.
        merged_analytics[canvas_user_id] = 'skipped'
        continue
    relative_submission_counts = {}
    for canvas_course_id, subs_grp in groupby(sites_grp, key=operator.itemgetter('canvas_course_id')):
        relative_submission_counts[canvas_course_id] = list(subs_grp)
    merge_advisee_assignment_submissions(advisee_terms_map, canvas_user_id, relative_submission_counts, canvas_site_map)
    merged_analytics[canvas_user_id] = 'merged'

ac.pop()
