from datetime import datetime as dt

from bson.objectid import ObjectId
from pymongo.database import Database

from ml_models.simulation_handler import SimulationHandler
from models.dataclass_models import (
    StockPossessData,
    Portfolio,
    Recommendation
)


class RecommendationHandler(object): 

    def __init__(
        self,
        db: Database,
        portfolio_id: str,
        date: dt,
    ):
        self.db = db
        portfolio_data = self.db.portfolio.find_one(
            {'_id': ObjectId(portfolio_id)}
        )
        if portfolio_data is None:
            ValueError("Cannot Find portfolio object")
        possess_list = portfolio_data.get('possess_list', [])
        possess_list = [StockPossessData(**x) for x in possess_list]
        simulation_result = portfolio_data.get('simulation_result')
        self.portfolio = Portfolio(
            _id=ObjectId(portfolio_id),
            initial_budget=portfolio_data['initial_budget'],
            current_budget=portfolio_data['current_budget'],
            possess_list=possess_list,
            simulation_result=simulation_result,
            reg_date=portfolio_data.get('reg_date')
        )
        sm_data = self.db.simulation_result.find_one(
            {'_id': simulation_result}
        )
        if not sm_data:
            ValueError("Cannot Find simulation object")
        self.sm_data = sm_data
        strategy_params = self.sm_data.get("strategy_params")
        self.smh = SimulationHandler(
            db=self.db,
            model_name=self.sm_data['model_name'],
            **strategy_params,
        )
        self.date = date

    def save_current_recommendation(self) -> None:
        try:
            multi_index_df, grouped = self.smh.prepare_recommendation_table(
                date=self.date
            )
        except Exception as e:
            error_message = f"Error while preparing df {str(e)}" 
            raise ValueError(error_message)

        try:
            sell_list = self.smh.return_recommendation_sell_list(
                self.date,
                multi_index_df=multi_index_df,
                groupby_data_list=grouped,
                stock_possess_list=self.portfolio.possess_list,
            )
        except Exception as e: 
            error_message = f"Error while retuurn sell stock_list {str(e)}" 
            raise ValueError(error_message)

        try:
            buy_list = self.smh.return_recommendation_buy_list(
                self.date,
                multi_index_df=multi_index_df,
                groupby_data_list=grouped,
            )
        except Exception as e: 
            error_message = f"Error while return sell stock_list {str(e)}" 
            raise ValueError(error_message)
        
        try:
            recommendation = Recommendation(
                simulation_result=self.portfolio.simulation_result, 
                portfolio=self.portfolio._id,
                buy_list=buy_list,
                sell_list=sell_list,
                date=self.date
            )
            recommend_json = recommendation.to_json
            self.db.recommendation.insert_one(recommend_json)
        except Exception as e: 
            error_message = f"Error while save data {str(e)}" 
            raise ValueError(error_message)

        


