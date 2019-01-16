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

from nessie.merged.sis_profile import get_merged_sis_profile


class TestMergedSisProfile:
    """Test merged SIS profile."""

    def test_skips_concurrent_academic_status(self, app, student_tables):
        """Skips concurrent academic status if another academic status exists."""
        profile = get_merged_sis_profile('11667051')
        assert profile['academicCareer'] == 'UGRD'

    def test_falls_back_on_concurrent_academic_status(self, app, student_tables):
        """Selects concurrent academic status if no other academic status exists."""
        profile = get_merged_sis_profile('1234567890')
        assert profile['academicCareer'] == 'UCBX'

    def test_withdrawal_cancel_ignored_if_empty(self, app, student_tables):
        profile = get_merged_sis_profile('11667051')
        assert 'withdrawalCancel' not in profile

    def test_withdrawal_cancel_included_if_present(self, app, student_tables):
        profile = get_merged_sis_profile('2345678901')
        assert profile['withdrawalCancel']['description'] == 'Withdrew'
        assert profile['withdrawalCancel']['reason'] == 'Personal'
        assert profile['withdrawalCancel']['date'] == '2017-03-31'

    def test_degree_progress(self, app, student_tables):
        profile = get_merged_sis_profile('11667051')
        assert profile['degreeProgress']['reportDate'] == '2017-03-03'
        assert len(profile['degreeProgress']['requirements']) == 4
        assert profile['degreeProgress']['requirements'][0] == {'entryLevelWriting': {'status': 'Satisfied'}}

    def test_no_holds(self, app, student_tables):
        profile = get_merged_sis_profile('11667051')
        assert profile['holds'] == []

    def test_multiple_holds(self, app, student_tables):
        profile = get_merged_sis_profile('2345678901')
        holds = profile['holds']
        assert len(holds) == 2
        assert holds[0]['reason']['code'] == 'CSBAL'
        assert holds[1]['reason']['code'] == 'ADVHD'
