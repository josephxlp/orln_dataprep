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


def clf_pointcloud(laz, lazo, method="smrf", a_srs=None):
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
    if a_srs:
        pipeline |= pdal.Writer.las(filename=lazo, compression="laszip",a_srs=f"EPSG:{a_srs}")
    else:
        pipeline |= pdal.Writer.las(filename=lazo, compression="laszip")
    #https://pdal.io/en/latest/stages/writers.las.html
    pipeline.execute()
    print(f"Reclassified point cloud saved to \n{lazo}")
    tf = time.perf_counter() - ti 
    print(f"run.time = {tf/60} min(s)")
    return lazo




def generate_dtm(laz, tif, res=10,outfn="mean", override_srs=None):
    ti = time.perf_counter()

    tif = tif.replace('.tif', f"_DTM{str(res)}_{outfn}.tif")

    if os.path.isfile(tif):
        print(f"file alredy exist...\n{tif}")
        return tif 
    """Extracts ground points and generates a DTM."""
    print(f"Generating DTM: {laz} \n-> {tif}")

    pipeline = pdal.Reader(laz)
    pipeline |= pdal.Filter.expression(expression="Classification == 2")
    if override_srs:
        
        pipeline |= pdal.Writer.gdal(
            filename=tif, gdalopts="tiled=yes, compress=deflate", nodata=-9999,
            output_type=outfn, resolution=res, override_srs=f"EPSG:{override_srs}"
        )
    else:
        pipeline |= pdal.Writer.gdal(
            filename=tif, gdalopts="tiled=yes, compress=deflate", nodata=-9999,
            output_type=outfn, resolution=res
        )
        
    pipeline.execute()
    tf = time.perf_counter() - ti 
    print(f"DTM saved to {tif}")
    print(f"run.time = {tf/60} min(s)")
    return tif


def generate_dsm_by_return(laz, tif, res=10, outfn="mean",override_srs=None):
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
    if override_srs:
        pipeline |= pdal.Writer.gdal(
            filename=tif, gdalopts="tiled=yes, compress=deflate", nodata=-9999,
            output_type=outfn, resolution=res, override_srs=f"EPSG:{override_srs}"
        )
    else:
        pipeline |= pdal.Writer.gdal(
            filename=tif, gdalopts="tiled=yes, compress=deflate", nodata=-9999,
            output_type=outfn, resolution=res
        )

    # Execute the pipeline
    pipeline.execute()
    tf = time.perf_counter() - ti 
    print(f"DSM saved to {tif}")
    print(f"Run time = {tf / 60:.2f} minute(s)")
    return tif


def generate_dtm_by_return(laz, tif, res=10, outfn="mean", override_srs=None):
    ti = time.perf_counter()

    # Define the output file name
    tif = tif.replace('.tif', f"_returnDTM_{str(res)}_{outfn}.tif")

    # Check if the file already exists
    if os.path.isfile(tif):
        print(f"File already exists...\n{tif}")
        return tif 

    print("Extracting last return points and generating a DTM.")
    print(f"Generating DTM by return: {laz} \n-> {tif}")

    # Define the PDAL pipeline
    pipeline = pdal.Reader(laz)

    # Use the return number filter to only include the last returns
    pipeline |= pdal.Filter.range(limits="returnnumber[-1:-1]")

    # Write to GeoTIFF with specified resolution and compression
    if override_srs:
        pipeline |= pdal.Writer.gdal(
            filename=tif, gdalopts="tiled=yes,compress=deflate", nodata=-9999,
            output_type=outfn, resolution=res, override_srs=f"EPSG:{override_srs}"
        )
    else:
        pipeline |= pdal.Writer.gdal(
            filename=tif, gdalopts="tiled=yes,compress=deflate", nodata=-9999,
            output_type=outfn, resolution=res
        )

    # Execute the pipeline
    pipeline.execute()
    tf = time.perf_counter() - ti 
    print(f"DTM saved to {tif}")
    print(f"Run time = {tf / 60:.2f} minute(s)")
    return tif
