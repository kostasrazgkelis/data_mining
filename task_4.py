from os import path
import pandas as pd
from sqlite3 import connect


def main():
    
    try:
        csv_file = pd.read_csv('joined_data.csv' , header=0)
    except Exception as e:
        print(f"There was an error {e}")
    else:
        conn = connect(':memory:')

        csv_file.to_sql('our_data', conn)




        # result = pd.read_sql("  SELECT total_events.[visitor_id]\
        #                         FROM (SELECT our_data.[visitor_id], MAX(our_data.[event_date]) as last_event\
        #                         FROM our_data \
        #                         GROUP BY our_data.[visitor_id] ) as total_events,\
        #                         ,    (  SELECT total_events.[visitor_id], MAX(total_events.[event_date]) as second_last_event\
        #                                 FROM total_events as new_table\
        #                                 WHERE second_last_event.[event_date] < (SELECT MAX(total_events.[event_date] FROM total_events WHERE second_last_event.[visitor_id] == total_events.[visitor_id])\
        #                                 )    "
        #                         , conn)

        # result = pd.read_sql("  SELECT *\
        #                         FROM (SELECT our_data.[visitor_id], MAX(our_data.[event_date]) as last_event, our_data.[event_date] as second_last_event\
        #                         FROM our_data \
        #                         WHERE second_last_event NOT IN (SELECT MAX(our_data.[event_date]) FROM our_data GROUP BY our_data.[visitor_id])\
        #                         GROUP BY  our_data.[visitor_id] ) as our_data", conn)

        # result = pd.read_sql("  SELECT our_data.[visitor_id], MAX(our_data.[event_date]) as second_last_event\
        #                         FROM our_data \
        #                         WHERE second_last_event NOT IN (SELECT MAX(our_data.[event_date]) FROM our_data GROUP BY our_data.[visitor_id])\
        #                         GROUP BY  our_data.[visitor_id] \
        #                          "
        #                         , conn)

        # result = pd.read_sql("  SELECT last_dates.[visitor_id], last_dates.[last_event]\
        #                         FROM (      SELECT our_data.[visitor_id] as visitor_id, MAX(our_data.[event_date]) as last_event\
        #                                     FROM our_data \
        #                                     GROUP BY our_data.[visitor_id] ) as last_dates,\
        #                         (           SELECT our_data.[visitor_id] as visitor_id, MAX(our_data.[event_date]) as second_last_event\
        #                                     FROM our_data \
        #                                     GROUP BY our_data.[visitor_id]\
        #                                     HAVING  second_last_event NOT IN (  SELECT MAX(our_data.[event_date]) \
        #                                                                         FROM our_data \
        #                                                                         GROUP BY our_data.[visitor_id]) \
        #                                     )  as second_last_dates\
        #                         "
        #                         , conn)


        result = pd.read_sql("  SELECT  our_data.[visitor_id], our_data.[event_date] \
                                FROM our_data \
                                GROUP BY our_data.[visitor_id]\
                                ORDER BY  our_data.[visitor_id] , our_data.[event_date]\
                                FETCH FIRST 3 ROWS ONLY" 
                        , conn)
        print(result)

if __name__ == "__main__":
    main()