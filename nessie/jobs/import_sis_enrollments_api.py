"""
Copyright ©2018. The Regents of the University of California (Regents). All Rights Reserved.

Permission to use, copy, modify, and distribute this software and its documentation
for educational, research, and not-for-profit purposes, without fee and without a
signed licensing agreement, is hereby granted, provided that the above copyright
notice, this paragraph and the following two paragraphs appear in all copies,
modifications, and distributions.

Contact The Office of Technology Licensing, UC Berkeley, 2150 Shattuck Avenue,
Suite 510, Berkeley, CA 94720-1620, (510) 643-7201, otl@berkeley.edu,
http://ipira.berkeley.edu/industry-info for commercial licensing opportunities.

IN NO EVENT SHALL REGENTS BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF REGENTS HAS BEEN ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.

REGENTS SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED HEREUNDER IS PROVIDED
"AS IS". REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
ENHANCEMENTS, OR MODIFICATIONS.
"""


"""Logic for SIS enrollments API import job."""


from flask import current_app as app
from nessie.externals import sis_enrollments_api
from nessie.jobs.background_job import BackgroundJob
from nessie.lib.berkeley import current_term_id, term_name_for_sis_id
from nessie.lib.queries import get_all_student_ids
from nessie.models import json_cache


class ImportSisEnrollmentsApi(BackgroundJob):

    def run(self, csids=None, term_id=None):
        if not csids:
            csids = [row['sid'] for row in get_all_student_ids()]
        if not term_id:
            term_id = current_term_id()
        app.logger.info(f'Starting SIS enrollments API import job for term {term_id}, {len(csids)} students...')

        json_cache.clear(f'term_{term_name_for_sis_id(term_id)}-sis_drops_and_midterms_%')

        success_count = 0
        failure_count = 0
        index = 1
        for csid in csids:
            app.logger.info(f'Fetching SIS enrollments API for SID {csid}, term {term_id} ({index} of {len(csids)})')
            if sis_enrollments_api.get_drops_and_midterms(csid, term_id):
                success_count += 1
            else:
                failure_count += 1
                app.logger.error(f'SIS enrollments API import failed for CSID {csid}.')
            index += 1
        app.logger.info(f'SIS enrollments API import job completed: {success_count} succeeded, {failure_count} failed.')
        return True
