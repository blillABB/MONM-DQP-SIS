"""Test script for conditional validation SQL generation"""
import yaml
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import only what we need, avoiding pandas dependencies
from validations.sql_generator import ValidationSQLGenerator, _annotate_expectation_ids

def main():
    # Load the test YAML
    with open('validation_yaml/Conditional_Validation_Example.yaml', 'r') as f:
        config = yaml.safe_load(f)

    # Annotate expectation IDs
    suite_name = config.get('metadata', {}).get('suite_name', '')
    config['validations'] = _annotate_expectation_ids(config.get('validations', []), suite_name)

    # Generate SQL
    generator = ValidationSQLGenerator(config)
    sql = generator.generate_sql(limit=10)

    # Print the generated SQL
    print('='* 80)
    print('GENERATED SQL FOR CONDITIONAL VALIDATIONS')
    print('='* 80)
    print(sql)
    print()
    print('='* 80)
    print('VERIFICATION CHECKS')
    print('='* 80)

    # Check for CTE
    if 'missing_critical_data_materials' in sql:
        print('✓ Derived group CTE "missing_critical_data_materials" found')
    else:
        print('✗ Derived group CTE NOT found')

    # Check for conditional membership checks
    if 'NOT IN (SELECT' in sql:
        print('✓ Conditional membership check found (NOT IN)')
    else:
        print('✗ Conditional membership check NOT found')

    # Check for regex validations
    if 'RLIKE' in sql:
        print('✓ Regex validations found')
    else:
        print('✗ Regex validations NOT found')

    # Count expectations
    validation_count = len([v for v in config['validations']])
    print(f'\n📊 Total validations defined: {validation_count}')

    derived_count = len(config.get('derived_statuses', []))
    print(f'📊 Total derived statuses: {derived_count}')

if __name__ == '__main__':
    main()
