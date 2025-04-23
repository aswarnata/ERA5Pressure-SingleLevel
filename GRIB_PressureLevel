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
    'year': ['2021', '2022'], # Ganti tahun yang ingin diproses
    'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
    'day': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10',
            '11', '12', '13', '14', '15', '16', '17', '18', '19', '20',
            '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31'],
    'variable': ['t', 'z'],  # Variabel yang diproses
    'pressure_level': ["900", "925", "950", "975", "1000"],
    'time': ['00:00', '03:00', '06:00', '09:00', '12:00', '15:00', '18:00', '21:00'],
    'raw_data_dir': 'Temp.Pressure', # Ubah ke directory yang berisi file GRIB
    'output_dir': 'Hasil_ERA5_TempPressure', # Ubah ke directory output yang diinginkan
    'shp_kabupaten': 'RBI_Indonesia/RBI_PROV_KAB6.shp', # Ubah ke path shapefile yang sesuai
}

os.makedirs(CONFIG['output_dir'], exist_ok=True)

class ERA5Processor: # Kelas utama yang memuat semua fungsi untuk pemrosesan data
    def __init__(self, config):
        self.config = config
        self.kabupaten = None
        self.load_batas_kabupaten()

    def load_batas_kabupaten(self):
        """"Fungsi untuk memmuat batas administrasi dan verifikasi jumlah kota/kab"""
        try:
            self.kabupaten = gpd.read_file(self.config['shp_kabupaten'])
            # Jika ingin testing, bisa tambahkan .head(jumlah kabupaten) di akhir baris

            jumlah_prov = self.kabupaten['WADMPR'].nunique()
            jumlah_kab = len(self.kabupaten)

            logger.info(f'Total provinsi: {jumlah_prov}, kabupaten: {jumlah_kab}')
            if jumlah_kab != 514:
                logger.warning('Jumlah Provinsi/Kabupaten tidak sesuai!')
        except Exception as e: # Logger untuk menunjukkan informasi pemrosesan error
            logger.error(f'Gagal memuat kabupaten: {e}')
            raise

    def grib_to_xarray(self, file_grib):
        """Fungsi untuk mengkonversi file GRIB ke xarray dataset"""
        try:
            return xr.open_dataset(file_grib, engine='cfgrib', decode_timedelta=True)
        except Exception as e:
            logger.error(f'Gagal mengonversi file GRIB ke dataset xarray: {e}')
            raise

    def calculate_weighted_average(self, datasetERA5, variables):
        """Fungsi untuk menghitung weighted averaged berdasarkan luas batas kabupaten"""
        try:
            data_kabupaten = []

            if self.kabupaten.crs != 'EPSG:4326':
                self.kabupaten = self.kabupaten.to_crs('EPSG:4326')

            lats, lons = datasetERA5.latitude.values, datasetERA5.longitude.values
            lat_res, lon_res = abs(lats[1] - lats[0]), abs(lons[1] - lons[0])
            transform = from_origin(lons.min() - lon_res / 2, lats.max() + lat_res / 2, lon_res, lat_res)

            # Menampilkan progress bar saat melakukan iterasi kabupaten
            for _, kabupaten in tqdm(self.kabupaten.iterrows(), total=len(self.kabupaten), desc="Processing Kabupaten"):
                kab_id, kab_name = kabupaten['KDBBPS'], kabupaten['WADMKK']

                mask = rasterize(
                    [(kabupaten.geometry, 1)],
                    out_shape=(len(lats), len(lons)),
                    transform=transform,
                    fill=0,
                    all_touched=True
                )

                for time in tqdm(datasetERA5.time.values, leave=False, desc=f"{kab_name[:12]} times"):
                    row_data = {
                        'district_id': kab_id,
                        'kota/kab': kab_name,
                        'year': pd.to_datetime(time).year,
                        'month': pd.to_datetime(time).month,
                        'day': pd.to_datetime(time).day,
                        'hour': pd.to_datetime(time).strftime('%H:%M')
                    }

                    # Menghitung area weighted averaged untuk setiap variabel
                    for variable in variables:
                        if variable not in datasetERA5:
                            continue

                        # Menambahkan prefix untuk nama kolom berdasarkan variabel
                        prefix_name = 'temp' if variable == 't' else 'geopot' if variable =='z' else variable

                        for pressure in datasetERA5.isobaricInhPa.values:
                            data_slice = datasetERA5[variable].sel(time=time, isobaricInhPa=pressure)
                            weighted_avg = np.nan if np.sum(mask) == 0 else np.sum(data_slice * mask) / np.sum(mask)
                            column_name = f'{prefix_name}{int(pressure)}hPa'
                            row_data[column_name] = float(weighted_avg) if not np.isnan(weighted_avg) else None

                    # Menambahkan data variabel ke list 
                    data_kabupaten.append(row_data)

            return pd.DataFrame(data_kabupaten)

        except Exception as e:
            logger.error(f'Failed to calculate weighted averages: {e}')
            raise

    def run(self):
        """Fungsi utama untuk memproses semua file GRIB dan menyimpan hasil ke CSV"""
        try:
            for year in self.config['year']:
                for month in self.config['month']:
                    file_grib = os.path.join(
                        self.config['raw_data_dir'],
                        f"Temp.Pressure.{year}{month}-{year}{month}.grib"
                    )

                    if not os.path.exists(file_grib):
                        logger.warning(f"{file_grib} not found, skipping...")
                        continue

                    datasetERA5 = self.grib_to_xarray(file_grib) # Konversi file ke xarray

                    df = self.calculate_weighted_average(datasetERA5, self.config['variable'])

                    nama_fileOutput = f"ERA5_TempPress_{year}-{month}.csv"
                    output_path = os.path.join(self.config['output_dir'], nama_fileOutput)
                    df.to_csv(output_path, index=False) # Konversi dataframe ke CSV
                    logger.info(f"File disimpan dengan nama: {nama_fileOutput}")


        except Exception as e:
            logger.error(f"Error saat menjalankan metode: {e}")
            raise

if __name__ == '__main__':
    ERA5Processor(CONFIG).run()
