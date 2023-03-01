#Reservoir status with Siri shortcut.
This is a project to fetch the status of the reservoir at Manzanares el Real from the web embalses.net
After fetch the data (updated from that web weekly) it write the value on a postgreSQL table.
I add all old data since 2020-01-05 on csv file.

This table can be used with metabase (https://metabase.com) to add dashboards and combine the info with weather data.

I also add a code to call via iOS shortcut to access an ssh and ask siri for the status of the reservoir, it return the status and the gap between the last 7 days.
