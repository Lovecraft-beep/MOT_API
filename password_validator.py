# Quick bit of Python to parse text list of common passwords 
# to see if they match MTS minimums

import re
from datetime import datetime

start=datetime.now()
in_rows = 0
v_pass = 0

# name of the input file
filepath = 'pwssd.txt'
#name of the output file
fileout = 'validpass.txt'
outF = open(fileout, "a")
#Loop
with open(filepath) as fp:
    line = fp.readline()
    while line:
        word = line.strip()
        wordlen = len(word)
        valid = 'TRUE'
        if wordlen<8:
            valid = 'FALSE'
        elif not re.search("[a-z]",word):
            valid = 'FALSE'
        elif not re.search("[A-Z]",word):
            valid = 'FALSE'
        elif not re.search("[0-9]",word):
            valid = 'FALSE'
        if valid == 'TRUE':
            print("Valid {}: {}".format(valid, word))
            v_pass = v_pass + 1
            outF.write(word)
            outF.write("\n")
            
        else:
            pass
        line = fp.readline()
        in_rows = in_rows + 1
outF.close()
fp.close
end = datetime.now()
time_taken = end - start
print('Time: ',time_taken)
print('Rows: ',in_rows)
print('Valid passwords ',v_pass)
print('End Of Line')