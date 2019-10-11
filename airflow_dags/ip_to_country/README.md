## IP To Country Mapping Data
DoltHub has an mapping of IP to country data that is updated daily. We get this from the

### Manual run
To update the data manually run the following command
```
$ python dolt_load/dolt_loader.py \
    --dolt_dir . \ 
    --push 
    --remote Liquidata/ip-to-country 
    --message "2019-10-01 update to dataset" 
    --commit 
    --clone ip_to_country
``` 
This will clone the latest, pull down the data, perform the updates, and then push the latest back to DoltHub.
### Airflow
Details to follow...