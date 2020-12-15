from flask_admin.contrib.mongoengine import (
    ModelView
)


class PortfolioView(ModelView):
    form_ajax_refs = {
        'simulation_result': {
            'fields': ['model_name']
        }
    }
