import cdsapi
import os
import calendar

output_path = "Download_ouput"

years = ['2023']
# untuk testing, ambil 4 bulan pertama
# contoh> months = [f"{i:02d}" for i in range (1, 5)]
months = [f"{i:02d}" for i in range (1, 13)]

# Untuk cek nama dataset yang digunakan bisa dilihat di CDS Request API di bagian bawah website download, sebelum submit
dataset = "reanalysis-era5-single-levels" 
variable = [ # Untuk cek nama variabel bisa dilihat di CDS Request API di bagian bawah website download, sebelum submit
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
        "2m_dewpoint_temperature",
        "2m_temperature",
        "mean_sea_level_pressure",
        "instantaneous_surface_sensible_heat_flux",
        "total_cloud_cover",
        "large_scale_rain_rate"
    ]
time = [
        "00:00", "03:00", "06:00",
        "09:00", "12:00", "15:00",
        "18:00", "21:00"
    ]

# Tambahkan pressure level untuk mendownload ERA5 Pressure Levels
# pressure_level =  [
#        "900", "925", "950",
#        "975", "1000"
#        ]

area = [7, 94, -12, 142] # Koordinat Indonesia [N, E, S, W]


client = cdsapi.Client()

for year in years:
    for month in months:
        _, jumlah_hari = calendar.monthrange(int(year), int(month))
        days = [f"{i:02d}" for i in range (1, jumlah_hari + 1)]

        output_file = os.path.join(output_path, f"ERA5SingleLevels.{year}{month}-{year}{month}.nc") # Penamaan bisa disesuaikan

        if os.path.exists(output_file):
            print(f"Skip {output_file}, sudah ada")
            continue

        print(f"Request data untuk tahun {year}-{month}")

        request_param = {
            'product_type': 'reanalysis',
            'format': 'netcdf', #ganti dengan 'grib' untuk ERA5 Pressure Levels
            'variable': variable,
            # tambahkan 'level_pressure': level_pressure > jika download ERA5 Pressure Level
            'year': year,
            'month': month,
            'day': days,
            'time': time,
            'area': area
        }
        
        client.retrieve(dataset, request_param, output_file)
        print(f"Download selesai: {output_file}")
