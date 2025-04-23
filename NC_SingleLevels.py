import os
import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
import logging
from rasterio.transform import from_origin
from rasterio.features import rasterize
from tqdm import tqdm

# Konfigurasi Logging (untuk debugging dan informasi dengan timestamp)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Konfigurasi
CONFIG = {
    'year': ['2023'],
    'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
    'variable': ['u10', 'v10', 'd2m', 't2m', 'msl', 'tcc', 'lsrr', 'ishf'],
    'time': ['00:00', '03:00', '06:00', '09:00', '12:00','15:00', '18:00', '21:00'],
    'raw_data_dir': 'File_ERA5_Raw', # Ubah ke directory yang berisi file GRIB
    'output_dir': 'Hasil_ERA5_SingleLevels', # Ubah ke directory output yang diinginkan
    'shp_kabupaten': 'RBI_Indonesia/RBI_PROV_KAB6.shp', # Ubah ke path shapefile yang sesuai
}

os.makedirs(CONFIG['output_dir'], exist_ok=True)

class ERA5Processor: # Kelas utama yang memuat semua fungsi untuk pemrosesan data
    def __init__(self, config):
        self.config = config
        self.kabupaten = None
        self.load_batas_kabupaten()

    def load_batas_kabupaten(self):
        """Load district boundaries from shapefile"""
        try:
            self.kabupaten = gpd.read_file(self.config['shp_kabupaten'])
            # Jika ingin testing, bisa tambahkan .head(jumlah kabupaten) di akhir baris
            jumlah_kab = len(self.kabupaten)
            logger.info(f'Total Kabupaten: {jumlah_kab}')
            if jumlah_kab != 514:
                logger.warning('Jumlah Provinsi/Kabupaten tidak sesuai!')
        except Exception as e:
            logger.error(f'Gagal memuat kabupaten: {e}')
            raise

    def nc_to_xarray(self, file_nc):
        """Fungsi untuk mengkonversi file NetCDF ke xarray dataset"""
        try:
            return xr.open_dataset(file_nc)
        except Exception as e:
            logger.error(f'Gagal mengonversi file NetCDF ke dataset xarray: {e}')
            raise

    def calculate_weighted_average(self, datasetERA5, variables):
        """Fungsi untuk menghitung weighted averaged berdasarkan luas batas kabupaten"""
        try:
            data_kabupaten = [] # List untuk menyimpan hasil per kabupaten
            # Penamaan variabel untuk nama kolom di output CSV
            variables_name = {
                't2m': 'temp_2m',
                'd2m': 'dewpoint_2m',
                'u10': 'wind_u_10m',
                'v10': 'wind_v_10m',
                'msl': 'mean_sea_level_pressure',
                'tcc': 'tot_cloud_cover',
                'lsrr': 'rain_rate',
                'ishf': 'instant_heat_flux'
            }

            # Memastikan sistem koordinat WGS 48 (EPSG:4326)
            if self.kabupaten.crs != 'EPSG:4326':
                self.kabupaten = self.kabupaten.to_crs('EPSG:4326')

            lats = datasetERA5.latitude.values
            lons = datasetERA5.longitude.values
            lat_res = abs(lats[1] - lats[0])
            lon_res = abs(lons[1] - lons[0])
            transform = from_origin(lons.min() - lon_res / 2, lats.max() + lat_res / 2, lon_res, lat_res)

            # Menampilkan progress bar saat melakukan iterasi kabupaten
            for _, kab in tqdm(self.kabupaten.iterrows(), total=len(self.kabupaten), desc="Processing Kabupaten"):
                kab_id, kab_name = kab['KDBBPS'], kab['WADMKK']
                
                mask = rasterize([(kab.geometry, 1)], 
                                 out_shape=(len(lats), len(lons)),
                                 transform=transform, 
                                 fill=0, 
                                 all_touched=True)

                for time in tqdm(datasetERA5['valid_time'].values, leave=False, desc=f"{kab_name[:12]} times"):
                    dt = pd.to_datetime(time)
                    row_data = {
                        'district_id': kab_id,
                        'kota/kab': kab_name,
                        'year': dt.year,
                        'month': dt.month,
                        'day': dt.day,
                        'hour': dt.strftime('%H:%M')
                    }
                    for var in self.config['variable']:
                    # Memastikan apakah variabel ada dalam dataset
                        if var in datasetERA5:
                            data_slice = datasetERA5[var].sel(valid_time=time)
                            # Menghitung weighted average sesuai dengan mask kabupaten
                            weighted_avg = np.nan if np.sum(mask) == 0 else np.sum(data_slice * mask) / np.sum(mask)
                            
                            col_name = variables_name.get(var, var)
                            row_data[col_name] = float(weighted_avg) if not np.isnan(weighted_avg) else None
                        else:
                            # Jika variabel tidak ada dalam dataset
                            col_name = variables_name.get(var, var)
                            row_data[col_name] = None

                    # Menambahkan data variabel ke list 
                    data_kabupaten.append(row_data)

            return pd.DataFrame(data_kabupaten)

        except Exception as e:
            logger.error(f'Gagal menghitung area weighted averaged: {e}')
            raise

    def run(self):
        """Fungsi utama untuk memproses semua file GRIB dan menyimpan hasil ke CSV"""
        try:
            for year in self.config['year']:
                for month in self.config['month']:
                    # Jika penamaan file asli berbeda, bisa disesuaikan di sini > f"nama file.nc"
                    file_nc = os.path.join(self.config['raw_data_dir'], f"ERA5SingleLevels.{year}{month}-{year}{month}.nc")
                    if not os.path.exists(file_nc):
                        logger.warning(f"File: {file_nc} tidak ditemukan, skipping...")
                        continue

                    datasetERA5 = self.nc_to_xarray(file_nc)
                    df = self.calculate_weighted_average(datasetERA5, variables=self.config['variable'])

                    # Penamaan file hasil, bisa disesuaikan > f"nama file.csv"
                    output_file = os.path.join(self.config['output_dir'], f"ERA5_SingleLevels_{year}{month}.csv")
                    df.to_csv(output_file, index=False) # Konversi dataframe ke CSV
                    logger.info(f"SFile disimpan dengan nama: {output_file}")
        except Exception as e:
            logger.error(f"Error saat menjalankan metode: {e}")
            raise

if __name__ == '__main__':
    ERA5Processor(CONFIG).run()
