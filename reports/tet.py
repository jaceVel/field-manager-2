
import zipfile

with zipfile.ZipFile(r'C:\Users\jstep\projects\field_manager\test data\olg ga\line 1/05_01_2025_17_06_21.zip') as zf:
    print(zf.namelist())