import os 
from glob import glob
import pdal 
import numpy as np 
import time 
from concurrent.futures import ProcessPoolExecutor, as_completed
from uvars import eba_laz_dir, eba_tif_dir,eba_lazclf_dir,transect_names

def pdal_quick_info(laz_path):
    reader = pdal.Reader(laz_path) # sensitive to resolution, can pass argument here 
    pipeline = reader.pipeline()
    qi = pipeline.quickinfo 
    return qi,pipeline


def pdal_pc_classes(pipeline):
    pipeline.execute()
    arrays = pipeline.arrays
    if len(arrays) == 0:
        print("No point data found in the file.")
    
    classification_values = arrays[0]['Classification']
    unique_classes = np.unique(classification_values)
    print(f"Unique classifications: {unique_classes}")
    return unique_classes

def clf_pointcloud(laz, lazo, method="smrf"):
    ti = time.perf_counter()
    print("""Classifies ground points in a point cloud and saves to LAZ.""")
    lazo = lazo.replace('.laz', f'_{method}.laz')

    if os.path.isfile(lazo):
        print(f"file already created {lazo}")
        return lazo 
    
    pipeline = pdal.Reader(laz)
    pipeline |= pdal.Filter.expression(expression="Classification != 7")
    pipeline |= pdal.Filter.assign(assignment="Classification[:]=0")
    if method == 'smrf':
        pipeline |= pdal.Filter.smrf()
    elif method == 'csf':
        pipeline |= pdal.Filter.csf()
    
    pipeline |= pdal.Writer.las(filename=lazo, compression="laszip")
    # https://pdal.io/en/latest/stages/writers.las.html
    pipeline.execute()
    print(f"Reclassified point cloud saved to \n{lazo}")
    tf = time.perf_counter() - ti 
    print(f"run.time = {tf/60} min(s)")
    return lazo

def generate_dsm_by_range(laz, tif, res=10, outfn="mean",lc=0,hc=4):
    ti = time.perf_counter()

    # Define the output file name
    tif = tif.replace('.tif', f"_rangeDSM{str(res)}_{outfn}_{lc}{hc}.tif")

    # Check if the file already exists
    if os.path.isfile(tif):
        print(f"File already exists...\n{tif}")
        return tif 

    print(f"Extracting points (classes {lc} to {hc}) and generating a DSM.")
    print(f"Generating DSM: {laz} \n-> {tif}")

    # Define the PDAL pipeline
    pipeline = pdal.Reader(laz)
    # Keep classifications 0 to 5 for DSM
    pipeline |= pdal.Filter.range(limits=f"Classification[{lc}:{hc}]")
    # Write to GeoTIFF with specified resolution and compression
    pipeline |= pdal.Writer.gdal(
        filename=tif,
        gdalopts="tiled=yes,compress=deflate",
        nodata=-9999,
        output_type=outfn,
        resolution=res
    )

    # Execute the pipeline
    pipeline.execute()
    tf = time.perf_counter() - ti 
    print(f"DSM saved to {tif}")
    print(f"Run time = {tf / 60:.2f} minute(s)")
    return tif

def generate_dsm_by_hag(lazo, dsm_tif, res, outfn):
    ti = time.perf_counter() 
    """Extracts highest points and generates a DSM."""
    # Define the output file name
    tif = dsm_tif = dsm_tif.replace('.tif', f"_hagDSM{str(res)}_{outfn}.tif")

    # Check if the file already exists
    if os.path.isfile(tif):
        print(f"File already exists...\n{tif}")
        return tif 

    pipeline = pdal.Reader(lazo)
    pipeline |= pdal.Filter.outlier(method="radius", radius=1.0, min_k=3)  # Remove high outliers
    # pipeline |= pdal.Filter.farthestneighbour()
    pipeline |= pdal.Filter.hag_delaunay()  # Use HAG Delaunay triangulation for DSM
    pipeline |= pdal.Writer.gdal(
        filename=dsm_tif, gdalopts="tiled=yes, compress=deflate", nodata=-9999,
        output_type=outfn, resolution=res
    )
    
    pipeline.execute()
    print(f"DSM saved to {dsm_tif}")
    tf = time.perf_counter() - ti 
    print(f"DSM saved to {tif}")
    print(f"Run time = {tf / 60:.2f} minute(s)")


def generate_dsm_by_return(laz, tif, res=10, outfn="mean"):
    ti = time.perf_counter()

    # Define the output file name
    tif = tif.replace('.tif', f"_returnDSM_{str(res)}_{outfn}.tif")

    # Check if the file already exists
    if os.path.isfile(tif):
        print(f"File already exists...\n{tif}")
        return tif 

    print("Extracting first return points and generating a DSM.")
    print(f"Generating DSM by return: {laz} \n-> {tif}")

    # Define the PDAL pipeline
    pipeline = pdal.Reader(laz)
    
    # Use the return number filter to only include the first returns
    pipeline |= pdal.Filter.range(limits="returnnumber[1:1]")
    
    # Write to GeoTIFF with specified resolution and compression
    pipeline |= pdal.Writer.gdal(
        filename=tif,
        gdalopts="tiled=yes,compress=deflate",
        nodata=-9999,
        output_type=outfn,
        resolution=res
    )

    # Execute the pipeline
    pipeline.execute()
    tf = time.perf_counter() - ti 
    print(f"DSM saved to {tif}")
    print(f"Run time = {tf / 60:.2f} minute(s)")
    return tif


def pointcloud_to_dsmtiff(laz,tif,lazo,res=10,outfn="mean",method="smrf",lc=0,hc=4,mode="range"):
    qi,pipeline = pdal_quick_info(laz)
    unique_classes = pdal_pc_classes(pipeline)
    # Check if 2 is in unique_classes
    if 2 in unique_classes:
        print('YES:::PointCloud ALREADY classified...')
        # generate_dtm(laz, tif, res=res,outfn=outfn)
        if mode == "range":
            generate_dsm_by_range(laz, tif, res=res, outfn=outfn,lc=lc,hc=hc)
        elif mode == "hag":
            generate_dsm_by_hag(lazo, tif, res, outfn)
        elif mode == "return":
            generate_dsm_by_return(laz, tif, res=res, outfn=outfn)
        else:
            print("mode not available...")
    else:
        print('NO:::PointCloud NOT YET classified...')
        lazo = clf_pointcloud(laz, lazo, method=method)
        # generate_dtm(lazo, tif, res=res,outfn=outfn)
        if mode == "range":
            generate_dsm_by_range(laz, tif, res=res, outfn=outfn,lc=lc,hc=hc)
        elif mode == "hag":
            generate_dsm_by_hag(lazo, tif, res, outfn)
        elif mode == "return":
            generate_dsm_by_return(laz, tif, res=res, outfn=outfn)
        else:
            print("mode not available...")

res, outfn, method,lc,hc = 10, "max", "csf",0,4 # max
mode1, mode2, mode3 = "range","return","hag"
num_workers = 5#20

def track_progress(future, total_files, batch_size, file_counter):
    file_counter += 1
    batch_num = (file_counter // batch_size) + 1
    print(f"Processing batch {batch_num} of {total_files // batch_size + 1} ...")
    return file_counter

if __name__ == "__main__":
    ti = time.perf_counter()
    total_files = sum([len(glob(f"{eba_laz_dir}/{location}/*.laz")) for location in transect_names])
    file_counter = 0

    for i, location in enumerate(transect_names):
        loc_laz_files = glob(f"{eba_laz_dir}/{location}/*.laz")
        loc_tif_dir = f"{eba_tif_dir}/{location}"
        loc_lazo_dir = f"{eba_lazclf_dir}/{location}"
        os.makedirs(loc_tif_dir, exist_ok=True)
        os.makedirs(loc_lazo_dir, exist_ok=True)
        print(f"{i}:{location} :: {len(loc_laz_files)} laz")
        
        with ProcessPoolExecutor(num_workers) as pex:
            futures = []
            for laz in loc_laz_files:
                btif = os.path.basename(laz).replace('.laz', '.tif')
                tif = f"{loc_tif_dir}/{btif}"
                lazo = f"{loc_lazo_dir}/{os.path.basename(laz)}"
                future = pex.submit(pointcloud_to_dsmtiff, laz, tif, lazo, res, outfn, method, lc, hc, mode2)
                future.add_done_callback(lambda fut: track_progress(fut, total_files, num_workers, file_counter))
                futures.append(future)

            # Wait for all futures to complete
            for future in as_completed(futures):
                pass

    tf = time.perf_counter() - ti 
    print(f"run.time = {tf/60} min(s)")
