from fp_types import (
    feature_list
)
from typing import (
    Dict,
    Tuple,
)
from fp_types import (
    BUY_ML_LOW,
)
from models.dataclass_models import MlModel, Strategy
from sklearn.base import BaseEstimator
from pymongo.database import Database
import pandas as pd
import pickle


class SimulationHandler(object): 

    def __init__(
        self,
        db: Database,
        model_name: str,
        minmum_purchase_diff: float = 0.1, 
        maximum_sell_diff: float = 0.02, 
        initial_total_budget: int = 3000000,
        single_purchase_amount: int = 100
    ):
        self.db = db
        self.model_name = model_name
        self.minmum_purchase_diff = minmum_purchase_diff
        self.maximum_sell_diff = maximum_sell_diff
        self.initial_total_budget = initial_total_budget
        self.single_purchase_amount = single_purchase_amount

    def simulate_model_result(
        self
    ) -> Dict:
        model_result = self.db.ml_model_result.find_one(
            {"model_name": self.model_name},
            {"_id": False}
        )
        ml_model = MlModel(**model_result)
        test_code_list = ml_model.test_code_list
        report_type = ml_model.report_type
        data_list = self.db.ml_feature_list.find(
            {
                "code": {"$in": test_code_list},
                "report_type": report_type
            },
            {"_id": False}
        )
        df = pd.DataFrame(list(data_list))
        with open(f'ml_models/mlfiles/{ml_model.model_name}', 'rb') as f:
            loaded_model = pickle.load(f)

        try:
            multi_index_df, groupby_data_list = self.prepare_simulation_df(
                df=df,
                loaded_model=loaded_model,
            )
        except Exception as e:
            raise ValueError(
                f"Error while prepare simulation df {str(e)}"
            )

        try:
            stock_possess_dict, total_budget = self.return_simulation_results(
                multi_index_df,
                groupby_data_list
            )
            return_percentage = self.calculate_final_return(
                stock_possess_dict,
                multi_index_df,
                total_budget
            )  
        except Exception as e:
            raise ValueError(
                f"Error while making simulation {str(e)}"
            ) 

        try:
            sl = Strategy(
                model_name=ml_model.model_name,
                report_type=ml_model.report_type,
                strategy_name=BUY_ML_LOW,
                strategy_params={
                    'minmum_purchase_diff': self.minmum_purchase_diff,
                    'maximum_sell_diff': self.maximum_sell_diff,
                    'single_purchase_amount': self.single_purchase_amount,
                },
                final_possess_products=stock_possess_dict,
                strategy_performance={
                    'final_return': return_percentage,
                    'initial_budget': self.initial_total_budget,
                    'final_budget': int(total_budget),
                }
            )
            sl.save(self.db.simulation_result)
            return sl.to_json
        except Exception as e:
            raise ValueError(
                f"Error while save data {str(e)}"
            )

    def prepare_simulation_df(
        self, 
        df: pd.DataFrame, 
        loaded_model: BaseEstimator
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df_x = df[feature_list]
        df['predicted_value'] = loaded_model.predict(df_x)
        multi_index_df = df.set_index(['code', 'stock_date'])
        multi_index_df['before_day_close'] = (
            multi_index_df
            .sort_index(1)
            .groupby(level=0)['Close']
            .shift(1)
        )
        multi_index_df['price_diff_percentage'] = (
            (
                multi_index_df['predicted_value'] 
                - 
                multi_index_df['before_day_close']
            ) / multi_index_df['before_day_close']
        )
        groupby_data_list = (
            multi_index_df.groupby(level=1).agg(
                {'price_diff_percentage': self.aggregate_func} 
            )
        )
        return multi_index_df, groupby_data_list

    def return_simulation_results(
        self,
        multi_index_df: pd.DataFrame,
        groupby_data_list: pd.DataFrame
    ) -> Tuple[
        Dict,
        int
    ]:
        total_budget = self.initial_total_budget
        stock_possess_dict = {}
        for index in groupby_data_list.index:
            for code in stock_possess_dict.keys():
                if (
                    multi_index_df['Close'][code].get(index) and
                    multi_index_df['price_diff_percentage'][code][index]
                    < self.maximum_sell_diff
                ):
                    stock_count = stock_possess_dict[code]
                    close_price = multi_index_df['Close'][code][index]
                    total_budget += stock_count * close_price
                    stock_possess_dict[code] = 0
            code_list = groupby_data_list['price_diff_percentage'][index]
            for code in code_list:
                if (
                    (
                        total_budget >
                        (
                            self.single_purchase_amount 
                            *
                            multi_index_df['Close'][code][index]
                        )
                    ) and (
                        multi_index_df['price_diff_percentage'][code][index]
                        > 
                        self.minmum_purchase_diff
                    ) 
                ):
                    total_budget -= (
                        self.single_purchase_amount * 
                        multi_index_df['Close'][code][index]
                    )
                    if stock_possess_dict.get(code):
                        stock_possess_dict[code] += self.single_purchase_amount
                    else:
                        stock_possess_dict[code] = self.single_purchase_amount

        return stock_possess_dict, total_budget

    def aggregate_func(self, x):
        largest_values = x.nlargest(3)
        if len(largest_values) > 0:
            return [y[0] for y in largest_values.index]
        return []   

    def calculate_final_return(
        self, 
        stock_possess_dict: Dict,
        multi_index_df: pd.DataFrame,
        total_budget: int
    ) -> float:
        final_budget = total_budget
        for code in stock_possess_dict:
            # 소유했던 주식의 가장 마지막 percentage를 계산함
            final_budget += (
                stock_possess_dict[code]
                * 
                (
                    multi_index_df['Close'][code][
                        multi_index_df['Close'][code].index[-1]
                    ]
                )
            )
        return (final_budget - self.initial_total_budget) / self.initial_total_budget