# ERA5 Pressure Levels & Single Levels
- Calculating monthly Temperature and Geopotential values at different pressure levels in Indonesia at district level using area weighted averaged.
- Calculating monthly 8 variables of ERA5 Single Levels values in Indonesia at district level using area weighted averaged.
  1. 10m u-component of wind,
  2. 10m v-component of wind,
  3. 2m dewpoint temperature,
  4. 2m temperature,
  5. Mean sea level pressure,
  6. Instantaneous surface sensible heat flux,
  7. Total cloud cover,
  8. Large scale rain rate

## 🔧 Requirements
- Python >= 3.8
- Dependencies:
- In python env
  ```bash
  pip install numpy pandas xarray geopandas rasterio tqdm cfgrib

- In conda prompt
  ```bash
  conda install -c conda-forge numpy pandas xarray geopandas rasterio tqdm cfgrib

## Download Source Material
https://cds.climate.copernicus.eu/datasets/reanalysis-era5-pressure-levels?tab=download

- Tutorial Download: 
https://youtu.be/ENlQuh9hw30

- File Format
  1. Pressure Level: **GRIB**
  2. Single Levels: **NetCDF4** *_karena pada GRIB file, ada variabel yang tidak bisa terbaca sehingga tidak bisa diproses._

## Note
Terdapat keterbatasan pemrosesan di Codespaces Github memiliki limit akses. Agar lebih aman bisa dijalankan di komputer lokal, hanya mengganti `Path file` (di bagian `#Konfigurasi`) ke directory penyimpanan file GRIB dan Shapefile batas administrasi di device masing-masing.
