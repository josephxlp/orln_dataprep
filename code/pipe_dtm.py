import os 
from glob import glob
import pdal 
import numpy as np 
import time 
from concurrent.futures import ProcessPoolExecutor
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

def generate_dtm(laz, tif, res=10,outfn="mean"):
    ti = time.perf_counter()

    tif = tif.replace('.tif', f"_DTM{str(res)}_{outfn}.tif")

    if os.path.isfile(tif):
        print(f"file alredy exist...\n{tif}")
        return tif 
    """Extracts ground points and generates a DTM."""
    print(f"Generating DTM: {laz} \n-> {tif}")

    pipeline = pdal.Reader(laz)
    pipeline |= pdal.Filter.expression(expression="Classification == 2")
    pipeline |= pdal.Writer.gdal(
        filename=tif, gdalopts="tiled=yes, compress=deflate", nodata=-9999,
        output_type=outfn, resolution=res
    )
    
    pipeline.execute()
    tf = time.perf_counter() - ti 
    print(f"DTM saved to {tif}")
    print(f"run.time = {tf/60} min(s)")
    return tif

def clf_pointcloud(laz, lazo, method="smrf"):
    ti = time.perf_counter()
    print("""Classifies ground points in a point cloud and saves to LAZ.""")
    lazo = lazo.replace('.laz', f'_{method}.laz')

    if os.path.isfile(lazo):
        print(f"file aready created {lazo}")
        return lazo 
    
    pipeline = pdal.Reader(laz)
    pipeline |= pdal.Filter.expression(expression="Classification != 7")
    pipeline |= pdal.Filter.assign(assignment="Classification[:]=0")
    if method == 'smrf':
        pipeline |= pdal.Filter.smrf()
    elif method == 'csf':
        pipeline |= pdal.Filter.csf()
    
    pipeline |= pdal.Writer.las(filename=lazo, compression="laszip")
    #https://pdal.io/en/latest/stages/writers.las.html
    pipeline.execute()
    print(f"Reclassified point cloud saved to \n{lazo}")
    tf = time.perf_counter() - ti 
    print(f"run.time = {tf/60} min(s)")
    return lazo

def pointcloud_to_dtmtiff(laz,tif,lazo,res=10,outfn="mean",method="smrf"):
    qi,pipeline = pdal_quick_info(laz)
    unique_classes = pdal_pc_classes(pipeline)
    # Check if 2 is in unique_classes
    if 2 in unique_classes:
        print('YES:::PointCloud ALREADY classified...')
        generate_dtm(laz, tif, res=res,outfn=outfn)
    else:
        print('NO:::PointCloud NOT YET classified...')
        lazo = clf_pointcloud(laz, lazo, method=method)
        generate_dtm(lazo, tif, res=res,outfn=outfn)
num_cores = 3
res, outfn, method= 10, "mean", "csf"#"smrf"
if __name__ == "__main__":
    ti = time.perf_counter()
    for i, location in enumerate(transect_names):
        #if i > 1: break
    
        loc_laz_files = glob(f"{eba_laz_dir}/{location}/*.laz")
        loc_tif_dir = f"{eba_tif_dir}/{location}"
        loc_lazo_dir = f"{eba_lazclf_dir}/{location}"
        os.makedirs(loc_tif_dir, exist_ok=True)
        os.makedirs(loc_lazo_dir, exist_ok=True)
        print(f"{i}:{location} :: {len(loc_laz_files)} laz")
        with ProcessPoolExecutor(num_cores) as pex:
            for laz in loc_laz_files:
                btif = os.path.basename(laz).replace('.laz', '.tif')
                tif = f"{loc_tif_dir}/{btif}"
                lazo = f"{loc_lazo_dir}/{os.path.basename(laz)}"
                #pointcloud_to_dtmtiff(laz,tif,lazo,res=10,outfn="mean",method="smrf")
                pex.submit(pointcloud_to_dtmtiff, laz,tif,lazo,res, outfn, method)

    tf = time.perf_counter() - ti 
    print(f"run.time = {tf/60} min(s)")