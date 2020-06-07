petovski_cash_dict = {
    'cashflow_from_operation': "((영업활동)|(영업활동으로[ \s]*인한)|(영업활동으로부터의))[ \s]*([순]*현금[ \s]*흐름)|營[ \s]*業[ \s]*活[ \s]*動[ \s]*.*現[ \s]*金[ \s]*",

}
petovski_income_statement_dict = {
    "operational_income": "^영[ \s]*업[ \s]*이[ \s]*익|\.[\s]*영[\s]*업[ \s]*이[ \s]*익|營[\s]*業[\s]*利[\s]*益",
    "gross_profit": "^매[ \s]*출[ \s]*총[ \s]*이[ \s]*익|매[\s]*출[\s]*총[\s]*이[\s]*익|賣[\s]*出[\s]*總[\s]*利[\s]*益[\s]*|매[\xa0]*출[\xa0]*총[\xa0]*이[\xa0]*익",
    "sales": "매출$|^영[ \s]*업[ \s]*수[ \s]*익|\.[ \s]*영[ \s]*업[ \s]*수[ \s]*익|^매[ \s]*출[ \s]*액|\.[ \s]*매[ \s]*출[ \s]*액|\(매출액\)|賣[ \s]*出[ \s]*額|매[ \s]*출[ \s]*액",
    'extra_ordinary_profit': "특[ \s]*별[ \s]*이[ \s]*익|特[\s]*別[\s]*利[\s]*益",
    'extra_ordinary_loss': "특[ \s]*별[ \s]*손[ \s]*실|特[\s]*別[\s]*損[\s]*失",
    "net_income": "^순[ \s]*이[ \s]*익|당[ \s]*기[ \s]*순[ \s]*이[ \s]*익|[ \s]*분[ \s]*기[ \s]*순[ \s]*이[ \s]*익|당\(분\)기순이익|당분기연결순이익|^純[\s]*利[\s]*益|當[\s]*期[\s]*純[\s]*利[\s]*益",
}

petovski_sub_income_dict = {
    "operational_sales": '^영[ \s]*업[ \s]*수[ \s]*익|\.[ \s]*영[ \s]*업[ \s]*수[ \s]*익',
    "operational_costs": '^영[ \s]*업[ \s]*비[ \s]*용|\.[ \s]*영[ \s]*업[ \s]*비[ \s]*용',
    "depreciation_cost": "감[ \s]*가[ \s]*상[ \s]*각",
    "corporate_tax_cost": "법[ \s]*인[ \s]*세[ \s]*비[ \s]*용",
    'cost_of_goods_sold': '재[\s]*고.*변[ \s]*동'
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


none_minus_keys = [
    'total_assets',
    'longterm_debt',
    'current_assets',
    'current_debt'
]