from worker.tasks import insert_all_report_data



if __name__ =="__main__":

    db_name = 'fp_data'

    insert_all_report_data(db_name)