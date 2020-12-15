petovski_cash_dict = {
    'cashflow_from_operation': "((영업활동)|(영업활동으로[ \s]*인한)|(영업활동으로부터의))[ \s]*([순]*현금[ \s]*흐름)|營[ \s]*業[ \s]*活[ \s]*動[ \s]*.*現[ \s]*金[ \s]*",

}
petovski_income_statement_dict = {
    "operational_income": "영[ \s]*업[ \s]*이[ \s]*익|\.[\s]*영[\s]*업[ \s]*이[ \s]*익|營[\s]*業[\s]*利[\s]*益|영[\s]*업[\s]*손[\s]*실|영[\xa0]*업[\xa0]*손[\xa0]*실",
    "gross_profit": "매[ \s]*출[ \s]*총[ \s]*이[ \s]*익|매[\s]*출[\s]*총[\s]*이[\s]*익|賣[\s]*出[\s]*總[\s]*利[\s]*益[\s]*|매[\xa0]*출[\xa0]*총[\xa0]*이[\xa0]*익|매[\s]*출[\s]*총[\s]*손[\s]*실|매[\xa0]*출[\xa0]*총[\xa0]*손[\xa0]*실",
    "sales": "매출$|^영[ \s]*업[ \s]*수[ \s]*익|\.[ \s]*영[ \s]*업[ \s]*수[ \s]*익|^매[ \s]*출[ \s]*액|\.[ \s]*매[ \s]*출[ \s]*액|\(매출액\)|賣[ \s]*出[ \s]*額|매[ \s]*출[ \s]*액",
    'extra_ordinary_profit': "특[ \s]*별[ \s]*이[ \s]*익|特[\s]*別[\s]*利[\s]*益",
    'extra_ordinary_loss': "특[ \s]*별[ \s]*손[ \s]*실|特[\s]*別[\s]*損[\s]*失",
    "net_income": "^순[ \s]*이[ \s]*익|당[ \s]*기[ \s]*순[ \s]*이[ \s]*익|[ \s]*[분|반|당][ \s]*기[ \s]*순[ \s]*이[ \s]*익|당\(분\)기순이익|당[ \s]*기[ \s]*순[ \s]*손[ \s]*실|당분기연결순이익|^純[\s]*利[\s]*益|當[\s]*期[\s]*純[\s]*利[\s]*益",
}

petovski_sub_income_dict = {
    "operational_sales": '^영[ \s]*업[ \s]*수[ \s]*익|\.[ \s]*영[ \s]*업[ \s]*수[ \s]*익',
    "operational_costs": '^영[ \s]*업[ \s]*비[ \s]*용|\.[ \s]*영[ \s]*업[ \s]*비[ \s]*용',
    "depreciation_cost": "감[ \s]*가[ \s]*상[ \s]*각",
    "corporate_tax_cost": "법[ \s]*인[ \s]*세[ \s]*비[ \s]*용|법[ \s]*인[ \s]*세",
    'cost_of_goods_sold': '재[\s]*고.*변[ \s]*동|매[\s]*출[ \s]*원[ \s]*가'
}

petovski_balancesheet_dict = {
    'book_value':"자[ \s]*본[ \s]*총[ \s]*계([ \s]*합[ \s]*계)*|자[ \s]*본[ \s]*총[ \s]*계|資[\s]*本[\s*]總[\s*]計[\s*]",
    'total_assets':"자[ \s]*산[ \s]*총[ \s]*계([ \s]*합[ \s]*계)*|자[ \s]*산[ \s]*총[ \s]*계|資[\s]*産[\s*]總[\s*]計[\s*]",
    "longterm_debt":"비[ \s]*유[ \s]*동[ \s]*부[ \s]*채|\.[ \s]*비[ \s]*유[ \s]*동[ \s]*부[ \s]*채|고[ \s]*정[ \s]*부[ \s]*채|固[ \s]*定[ \s]*負[ \s]*債[ \s]*",
    "current_assets":"유[\s]*동[\s]*자[\s]*산|\.[ \s]*유[ \s]*동[ \s]*자[ \s]*산([ \s]*합[ \s]*계)*|流[ \s]*動[ \s]*資[ \s]*産",
    "current_debt":"유[ \s]*동[ \s]*부[ \s]*채|\.[ \s]*유[ \s]*동[ \s]*부[ \s]*채([ \s]*합[ \s]*계)*|流[\s]*動[\s]*負[\s]*債",
}
petovski_re_dict={
    'cashflow':petovski_cash_dict, 
    'income_statement':petovski_income_statement_dict,
    'balance_sheet':petovski_balancesheet_dict,
}


total_debt_re_key = "부[ \s]*채[ \s]*총[ \s]*계"
current_assets_res = {
    "cashlike_assets": '현[\s]*금[\s]*.*자[\s]*산',
    "short_term_finance_assets": '단[\s]*기[\s].*금[\s]*융[\s]*자[\s]*산|단[\s]*기.*[\s]*금[\s]*융[\s]*상[\s]*품',
    "current_term_finance_assets": '당기[\s]*기[\s].*금[\s]*융[\s]*자[\s]*산|당[\s]*기.*[\s]*금[\s]*융[\s]*상[\s]*품',
    "bond_assets": '대[\s]*출[\s]*채[\s]*권|매[\s]*출[\s]*채[\s]*권',
    "account_receivable": "미[ \s]*수[ \s]*금",
    'advance_payment': '선[\s]*급[ \s]*금',
    'asset_to_be_sold': '매[\s]*각[\s].*[\s]*자[\s]*산',
    'inventory_assets':'재[\s]*고[\s].*[\s]*자[\s]*산'
}
additional_balance_data =  {
    'total_debt': "부[ \s]*채[ \s]*총[ \s]*계",
    'long_term_debt': "고[ \s]*정[ \s]*부[ \s]*채",
    "cashlike_assets": '현[\s]*금[\s]*.*자[\s]*산',
    "short_term_finance_assets": '단[\s]*기[\s].*금[\s]*융[\s]*자[\s]*산|단[\s]*기.*[\s]*금[\s]*융[\s]*상[\s]*품',
    "current_term_finance_assets": '당기[\s]*기[\s].*금[\s]*융[\s]*자[\s]*산|당[\s]*기.*[\s]*금[\s]*융[\s]*상[\s]*품',
    "bond_assets": '대[\s]*출[\s]*채[\s]*권|매[\s]*출[\s]*채[\s]*권',
    "account_receivable": "미[ \s]*수[ \s]*금",
    'advance_payment': '선[\s]*급[ \s]*금',
    'asset_to_be_sold': '매[\s]*각[\s].*[\s]*자[\s]*산',
    'inventory_assets':'재[\s]*고[\s].*[\s]*자[\s]*산',
    "deposit": '예[\s]*수[\s]*금|예[\s]*수[\s]*부[\s]*채',
    "short_term_finance_debt": '단[\s]*기[\s].*금[\s]*융[\s]*부[\s]*채',
    "current_term_finance_assets": '당기[\s]*기[\s].*금[\s]*융[\s]*부[\s]*채',
    "bond_assets": '대[\s]*출[\s]*채[\s]*권|매[\s]*출[\s]*채[\s]*권',
    "account_payable": "매[ \s]*입[ \s]*채[ \s]*무|미[ \s]*지[ \s]*급[ \s]*금",
    'advance_payment': '선[\s]*급[ \s]*금',
    'asset_to_be_sold': '매[\s]*각[\s].*[\s]*자[\s]*산',
    'inventory_assets':'재[\s]*고[\s].*[\s]*자[\s]*산'
}

current_debt_res = {
    "deposit": '예[\s]*수[\s]*금|예[\s]*수[\s]*부[\s]*채',
    "short_term_finance_debt": '단[\s]*기[\s].*금[\s]*융[\s]*부[\s]*채',
    "current_term_finance_assets": '당기[\s]*기[\s].*금[\s]*융[\s]*부[\s]*채',
    "bond_assets": '대[\s]*출[\s]*채[\s]*권|매[\s]*출[\s]*채[\s]*권',
    "account_payable": "매[ \s]*입[ \s]*채[ \s]*무|미[ \s]*지[ \s]*급[ \s]*금",
    'advance_payment': '선[\s]*급[ \s]*금',
    'asset_to_be_sold': '매[\s]*각[\s].*[\s]*자[\s]*산',
    'inventory_assets':'재[\s]*고[\s].*[\s]*자[\s]*산'
}


none_minus_keys = [
    'total_assets',
    'longterm_debt',
    'current_assets',
    'current_debt',
    'book_value'
]
balance_data_re_list = [
    f"{petovski_balancesheet_dict['current_assets']}"
    f"|{petovski_balancesheet_dict['total_assets']}",
    petovski_balancesheet_dict['current_debt'],
]
income_data_re_list = [
    petovski_income_statement_dict['operational_income'],
    petovski_income_statement_dict['gross_profit']
]
cashflow_data_re_list = [
    petovski_cash_dict['cashflow_from_operation']
]
summary_re_list = [
    petovski_balancesheet_dict['current_assets'],
    petovski_balancesheet_dict['current_debt'],
    petovski_income_statement_dict['operational_income'],
    petovski_income_statement_dict['net_income']
]

BALANCE_RE = "재[ \s]*무[ \s]*상[ \s]*태[ \s]*표|대[ \s]*차[ \s]*대[ \s]*조[ \s]*표|재[ \s]*무[ \s]*상[ \s]*태[ \s]*표"
INCOME_RE = "손[\s]*익[\s]*계[\s]*산[\s]*서|포[ \s]*괄[ \s]*손[ \s]*익[ \s]*계[ \s]*산[ \s]*서"
CASHFLOW_RE = "현[ \s]*금[ \s]*흐[ \s]*름[ \s]*표"
CONNECTED_BALANCE_RE = "연[\s]*결[\s]*재[ \s]*무[ \s]*상[ \s]*태[ \s]*표|연[ \s]*결[ \s]*대[ \s]*차[ \s]*대[ \s]*조[ \s]*표"
CONNECTED_INCOME_RE =  "연[ \s]*결[\s]*손[\s]*익[\s]*계[\s]*산[\s]*서|연[ \s]*결[\s]*포[ \s]*괄[ \s]*손[ \s]*익[ \s]*계[ \s]*산[ \s]*서"
CONNECTED_CASHFLOW_RE = "연[ \s]*결[ \s]*현[ \s]*금[ \s]*흐[ \s]*름[ \s]*표"