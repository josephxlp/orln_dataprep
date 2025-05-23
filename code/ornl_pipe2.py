import os
import concurrent.futures
import pandas as pd
import numpy as np
from glob import glob
import pdal

from utils import generate_dtm,generate_dsm_by_return,clf_pointcloud
# Define a function that processes each row
def process_row(i, row, ornl_tif_dir, ornl_lazcfl_dir):
    logdir="/home/ljp238/Downloads/brazil_ornl/ORNL/code/log_ornl"
    os.makedirs(logdir, exist_ok=True)
    #state = row['state']
    laz = row['fullpath']
    filename = row['filename']
    epsgcode = row['epsg_code'] 
    uclasses = row['uc']
    #uclasses = convert_to_int_list(uclasses)
    print(f"Processing {i+1} - {uclasses}")
    
    loc_dpath = os.path.join(ornl_tif_dir, 'state')
    os.makedirs(loc_dpath, exist_ok=True)
    tif = os.path.join(loc_dpath, os.path.basename(laz).replace('.laz', '.tif'))
    lazo = os.path.join(ornl_lazcfl_dir, os.path.basename(laz))

    try:

        try:
            generate_dtm(laz, tif, res=10, outfn="mean", override_srs=epsgcode)
            generate_dsm_by_return(laz, tif, res=10, outfn="max", override_srs=epsgcode)
        except Exception:
            print('Noooooooooooooooooooooooooooooooooooooooooo')
            lazo = clf_pointcloud(laz, lazo, method="csf", a_srs=epsgcode)
            generate_dtm(laz, tif, res=10, outfn="mean", override_srs=epsgcode)
            generate_dsm_by_return(laz, tif, res=10, outfn="max", override_srs=epsgcode)
    except Exception as e:
        print('==================================================')
        print(f"Error processing {os.path.basename(laz)}: {e}")
        print('==================================================')
        txtfile = f"{logdir}/{i}_{filename}_error_log.txt"
        with open(txtfile, "a") as f:
            #f.write(f"Error processing {os.path.basename(laz)}: {e}\n")
            f.write(laz + "\n")


#ThreadPoolExecutor ###ProcessPoolExecutor
# Define a function for batch processing
def process_batches(df, ornl_tif_dir, ornl_lazcfl_dir, num_workers=4):
    batch_size = len(df)  # The total number of rows to process
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for i, row in df.iterrows():
            futures.append(executor.submit(process_row, i, row, ornl_tif_dir, ornl_lazcfl_dir))
            if (i + 1) % batch_size == 0:  # Print when each batch is processed
                print(f"Processing batch {i + 1}/{batch_size}")

        # Wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            future.result()  # Ensures any exception is raised

# Call the function with the DataFrame, directories, and desired number of workers



ornl_laz_dir = "/media/ljp238/12TBWolf/ARCHIEVE/Brazil_ORNL_laz/"
ornl_laz_files = glob(ornl_laz_dir + "*.laz")
#ornl_tif_dir = "/media/ljp238/12TBWolf/ARCHIEVE/Brazil_ORNL_tif/laz2tif"
ornl_tif_dir = "/media/ljp238/12TBWolf/ARCHIEVE/Brazil_ORNL_tif/meta_stateless"
ornl_lazcfl_dir = "/media/ljp238/12TBWolf/ARCHIEVE/Brazil_ORNL_tif/laz_clf"
os.makedirs(ornl_tif_dir, exist_ok=True)
os.makedirs(ornl_lazcfl_dir, exist_ok=True)

# clean up here 
# create vector tiles , and use it to download tiles 90m project 
if __name__ == "__main__":
    df = pd.read_csv('meta_stateless.csv')
    process_batches(df, ornl_tif_dir, ornl_lazcfl_dir, num_workers=1) #30
    print("Finshed")




# forge the bad files for now, just work with the good files
# then work on the bad files later





