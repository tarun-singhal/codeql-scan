"""Self Audit class for the version 2 template."""
from __future__ import annotations

import traceback
from dataclasses import dataclass

import pandas as pd
from pymysql import Error

from lib.agrior_lib import AgricorLib
from model.self_audit_model_v2 import SelfAuditModelV2
from service.self_audit_service_v2 import SelfAuditServiceV2
from validator.license_type_vertical_validator import LicenseTypeVerticalValidator
from validator.self_audit_validator_v2 import SelfAuditValidatorV2


@dataclass
class SelfAuditReaderV2(AgricorLib):

    """Self audit class reader."""

    zip_code_obj = ''
    self_audit_service = ''
    self_audit_validator = ''
    COMPLIANT_ANS = 'compliant answer'
    NON_COMPLIANT_ANS = 'non-compliant answer'
    data_compliance = ''
    self_audit_obj = SelfAuditModelV2()
    self_audit_service = SelfAuditServiceV2()
    self_audit_validator_2 = LicenseTypeVerticalValidator()
    saq_validator = SelfAuditValidatorV2()

    def initial_clean_up(self):
        """Make initial cleanup in db, if required."""
        # ATTENTION: Truncate table would not be happen if we have multiple sheets.
        self.self_audit_obj.truncate_table_by_force(
            'TRUNCATE TABLE PolicyRequirementDB.policy_compliances',
        )
        self.self_audit_obj.truncate_table_by_force(
            'TRUNCATE TABLE PolicyRequirementDB.policy_requirements',
        )

        self.self_audit_obj.truncate_table_by_force(
            'TRUNCATE TABLE PolicyRequirementDB.audit_categories',
        )
        self.self_audit_obj.truncate_table_by_force(
            'TRUNCATE TABLE PolicyRequirementDB.policy_requirement_verticals',
        )
        self.self_audit_obj.truncate_table_by_force(
            'TRUNCATE TABLE PolicyRequirementDB.policy_requirement_license_types',
        )
        self.self_audit_obj.truncate_table_by_force(
            'TRUNCATE TABLE PolicyRequirementDB.policy_requirement_permits',
        )
        self.self_audit_obj.truncate_table_by_force(
            'TRUNCATE TABLE PolicyRequirementDB.policy_vertical_techniques',
        )
        return True

    def make_clean_db(self):
        """Make clean the db tables."""
        print('DB cleanup activity started...')
        self.initial_clean_up()
        print('DB cleanup activity done')
        return True

    def file_validators(self, file_name='', output=False):
        """Validate the file on desired points."""
        # TODO: Need to move the file column specifc error in extra sheet into output_error file
        df = pd.read_excel(file_name, sheet_name=None)
        sheet_name = self.saq_validator.check_sheet_name(df)
        if sheet_name is not False:
            df = pd.read_excel(file_name, sheet_name=sheet_name)
            df.columns = df.columns.str.strip()

            # Check the valid column passed in the sheet
            column_resp = self.saq_validator.validate_saq_column(df)
            if column_resp is not True:
                print(column_resp)
                return output

            # Remove NA and rename the sheet column
            df_deep_copy = df.copy(deep=True)
            df = self.self_audit_service.rename_sheet_column(df)
            df = df.fillna('')

            # validate the file
            resp = self.saq_validator.validate_content(df, df_deep_copy, file_name)
            if resp is True:
                print('Validation Check: .............................. PASSED ')
                # input_key = self.ask_data_cleanup()
                # if input_key == 'y':
                #    self.make_clean_db()
                self.execute(df, df_deep_copy, file_name)
                return True
            else:
                print('Validation Check: .............................. FAILED ')
                print('Please check the error generated file in saq_error directory...')
                return False
        return True

    def execute(self, df: object, df_deep_copy: object, file_path: str):
        """Start the process of file execution."""
        try:
            print(f'Execution started on {file_path}')
            if df_deep_copy.columns.isin(['Issues']).any():
                df_deep_copy = df_deep_copy.drop('Issues', axis=1)
            file_path = self.set_file_path(file_path, 'saq_success/', 'SAQ_Id')
            # ATTENTION: Truncate table would not be happen if we have multiple sheets.
            for index, row in df.iterrows():
                print(f'Processing row : {index}')
                if row['action'].strip().lower() == 'add':                    
                    requirement_id = self.self_audit_obj.create_policy_requirement(row, '', '')
                    if requirement_id > 0:
                        self.make_entry_in_db(requirement_id, row)
                elif row['action'].strip().lower() == 'delete':
                    self.self_audit_obj.delete_policy_requirement(row['policy_requirement_id'])
                elif row['action'].strip().lower() == 'update':
                    requirement_id = self.self_audit_obj.update_policy_requirement(row)
                    if requirement_id > 0:
                       self.make_entry_in_db(requirement_id, row)
                #else:
                #    #print('ISSUE FOUND !!')
                #    return False
                
        except Error as error:
            print(error)
        return True

    def make_entry_in_db(self, requirement_id: int, row: object) -> None:
        """Make entry into DB."""
        try:
            # add entry for the policy license_type and verticals
            self.self_audit_obj.create_policy_license_type(row, requirement_id)
            # add entry for the policy permits
            if row['permit'] != '':
                self.self_audit_obj.create_policy_permit(row, requirement_id)

            # add entry for the policy compliances questions
            self.policy_compliance_handle(row, requirement_id, validation=False)
        except BaseException as e:
            print(f'ERROR FOUND: {e}')
            print(f'STACK TRACE: {traceback.format_exc()}')

    def policy_compliance_handle(self, row: object, requirement_id: int, validation=True) -> bool:
        """Check for the policy compliance exists."""
        audit_response = row['initial_audit_response'].strip()
        non_compliant = 'No' if audit_response == 'Yes' else 'Yes'

        policy_compliance = {}
        policy_compliance['requirement_id'] = requirement_id
        policy_compliance['question'] = row['initial_audit_question'].strip()
        policy_compliance['non_compliant'] = non_compliant.strip()
        policy_compliance['action'] = row['initial_action_for_non_compliance'].strip(
        )
        policy_compliance['level'] = '1'
        policy_compliance['trigger_response'] = ''

        if validation is False:
            # check for the initial action for non-compliance, store it in policy_compliances table
            self.self_audit_obj.create_policy_compliance(policy_compliance)
        else:
            resp_id = self.self_audit_obj.check_policy_compliance_exists(
                policy_compliance,
            )
            if resp_id is False:  # means record not found
                return False

        # check for the second level of question if first level question found in db
        secondary_qns = row['response_for_secondary_audit_question']

        if secondary_qns.replace('"', '').lower() == 'yes' or secondary_qns.strip() != '':
            # insert record into policy_compliance table for the non-compliance
            audit_response = row['secondary_audit_response'].strip()
            non_compliant = 'No' if audit_response == 'Yes' else 'Yes'
            policy_compliance['level'] = '2'
            # removing leading and trailing spaces
            policy_compliance['non_compliant'] = non_compliant.strip()
            policy_compliance['question'] = row['secondary_audit_question'].strip()
            policy_compliance['action'] = row['secondary_audit_for_non_compliance'].strip(
            )
            policy_compliance['trigger_response'] = secondary_qns.replace('"', '',).strip().lower().capitalize()
            if validation is False:
                self.self_audit_obj.create_policy_compliance(policy_compliance)
                return True
            else:
                resp_id = self.self_audit_obj.check_policy_compliance_exists(policy_compliance)
                if resp_id is False:
                    return False
                return resp_id
        return True
