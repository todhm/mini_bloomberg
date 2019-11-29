from fpcrawler.settings import * 
import os 

DATABASES['default']['DB_NAME'] = 'test_fp_data'
LOGGING['handlers']['mongo']['database_name'] = 'test_fp_data'


MIGRATION_MODULES = {
    'auth': None,
    'contenttypes': None,
    'default': None,
    'sessions': None,
    'core': None,
    'profiles': None,
    'snippets': None,
    'scaffold_templates': None,
}
