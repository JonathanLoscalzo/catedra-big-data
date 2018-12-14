# Para generar el streaming
## option 1
'''
stdbuf –oL python vivero_streaming.py | nc –lk 7777
'''

## option 2
'''
mkfifo /tmp/pipe
nc -lkp 7777 < /tmp/pipe | python traffic_stream.py > /tmp/pipe
'''

