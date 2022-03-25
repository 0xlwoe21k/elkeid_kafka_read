# usage:
```
[root@elkeid ~]# ./kafkaReadFromElkeid_linux  -h
flag needs an argument: -h
Usage of ./kafkaReadFromElkeid_linux:
  -h string
    	kafka address. (default "10.43.48.22:9092")
  -p string
    	sasl_password. (default "elkeid")
  -s string
    	use sasl. (default "true")
  -t string
    	kafka topic. (default "hids_svr")
  -u string
    	sasl_username. (default "admin")
```

```
[root@elkeid ~]# ./kafkaReadFromElkeid_linux  -s true -t hids_svr -u admin -p elkeid -h 10.43.48.22:9092
```
