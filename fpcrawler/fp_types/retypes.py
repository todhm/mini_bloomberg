petovski_cash_dict = {
    "((영업활동)|(영업활동으로[ \s]*인한)|(영업활동으로부터의))[ \s]*([순]*현금[ \s]*흐름)":'cashflow_from_operation',

}
petovski_income_statement_dict = {
    "^영[ \s]*업[ \s]*이[ \s]*익|\.[ \s]*영[ \s]*업[ \s]*이[ \s]*익":"operational_income",
    "^영[ \s]*업[ \s]*수[ \s]*익|\.[ \s]*영[ \s]*업[ \s]*수[ \s]*익":"operational_sales",
    "^영[ \s]*업[ \s]*비[ \s]*용|\.[ \s]*영[ \s]*업[ \s]*비[ \s]*용":"operational_costs",
    "^매[ \s]*출[ \s]*총[ \s]*이[ \s]*익|\.[ \s]*매[ \s]*출[ \s]*총[ \s]*이[ \s]*익":"gross_profit",
    "^매[ \s]*출[ \s]*액|\.[ \s]*매[ \s]*출[ \s]*액|\(매출액\)":"sales",
    "특[ \s]*별[ \s]*이[ \s]*익":'extra_ordinary_profit',
    "특[ \s]*별[ \s]*손[ \s]*실":'extra_ordinary_loss',
    "^순[ \s]*이[ \s]*익|당[ \s]*기[ \s]*순[ \s]*이[ \s]*익|[ \s]*분[ \s]*기[ \s]*순[ \s]*이[ \s]*익|당\(분\)기순이익|당분기연결순이익":"net_income"
    
}
petovski_balancesheet_dict = {
    "자[ \s]*산[ \s]*총[ \s]*계([ \s]*합[ \s]*계)*|자[ \s]*산[ \s]*총[ \s]*계":'total_assets',
    "^비[ \s]*유[ \s]*동[ \s]*부[ \s]*채|\.[ \s]*비[ \s]*유[ \s]*동[ \s]*부[ \s]*채|고[ \s]*정[ \s]*부[ \s]*채":"longterm_debt",
    "^유[ \s]*동[ \s]*자[ \s]*산([ \s]*합[ \s]*계)*|\.[ \s]*유[ \s]*동[ \s]*자[ \s]*산([ \s]*합[ \s]*계)*":"current_assets",
    "^유[ \s]*동[ \s]*부[ \s]*채([ \s]*합[ \s]*계)*|\.[ \s]*유[ \s]*동[ \s]*부[ \s]*채([ \s]*합[ \s]*계)*":"current_debt",
}
petovski_re_dict={
    'cashflow_dict':petovski_cash_dict, 
    'income_statement':petovski_income_statement_dict,
    'balancesheet':petovski_balancesheet_dict,
}
