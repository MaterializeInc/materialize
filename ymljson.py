import json, yaml

yml = """
    512C:
      cpu_limit: 62
      memory_limit: 481280MiB
      disk_limit: 3481600MiB
      scale: 32
      workers: 62
      credits_per_hour: "512"
    256C:
      cpu_limit: 62
      memory_limit: 481280MiB
      disk_limit: 3481600MiB
      scale: 16
      workers: 62
      credits_per_hour: "256"
    128C:
      cpu_limit: 62
      memory_limit: 481280MiB
      disk_limit: 3481600MiB
      scale: 8
      workers: 62
      credits_per_hour: "128"
    6400cc:
      cpu_limit: 62
      memory_limit: 481280MiB
      disk_limit: 3481600MiB
      scale: 4
      workers: 62
      credits_per_hour: "64"
    3200cc:
      cpu_limit: 62
      memory_limit: 481280MiB
      disk_limit: 3481600MiB
      scale: 2
      workers: 62
      credits_per_hour: "32"
    1600cc:
      cpu_limit: 62
      memory_limit: 481280MiB
      disk_limit: 3481600MiB
      scale: 1
      workers: 62
      credits_per_hour: "16"
    1200cc:
      cpu_limit: 46
      memory_limit: 357078MiB
      disk_limit: 2583122MiB
      scale: 1
      workers: 46
      credits_per_hour: "12"
    800cc:
      cpu_limit: 30
      memory_limit: 232877MiB
      disk_limit: 1684645MiB
      scale: 1
      workers: 30
      credits_per_hour: "8"
    600cc:
      cpu_limit: 23
      memory_limit: 178539MiB
      disk_limit: 1291561MiB
      scale: 1
      workers: 23
      credits_per_hour: "6"
    400cc:
      cpu_limit: 15
      memory_limit: 116438MiB
      disk_limit: 842322MiB
      scale: 1
      workers: 15
      credits_per_hour: "4"
    200cc:
      cpu_limit: 7
      memory_limit: 54338MiB
      disk_limit: 393083MiB
      scale: 1
      workers: 7
      credits_per_hour: "2"
    100cc:
      cpu_limit: 3.5
      memory_limit: 27169MiB
      disk_limit: 196541MiB
      scale: 1
      workers: 4
      credits_per_hour: "1"
    50cc:
      cpu_limit: 1.75
      memory_limit: 13584MiB
      disk_limit: 98270MiB
      scale: 1
      workers: 2
      credits_per_hour: "0.5"
    25cc:
      cpu_limit: 0.875
      memory_limit: 6792MiB
      disk_limit: 49135MiB
      scale: 1
      workers: 1
      credits_per_hour: "0.25"
    # Legacy cluster sizings
    6xlarge:
      cpu_limit: 62
      memory_limit: 470GiB
      disk_limit: 3400GiB
      scale: 32
      workers: 56
      credits_per_hour: "512"
    5xlarge:
      cpu_limit: 62
      memory_limit: 470GiB
      disk_limit: 3400GiB
      scale: 16
      workers: 56
      credits_per_hour: "256"
    4xlarge:
      cpu_limit: 62
      memory_limit: 470GiB
      disk_limit: 3400GiB
      scale: 8
      workers: 56
      credits_per_hour: "128"
    3xlarge:
      cpu_limit: 62
      memory_limit: 470GiB
      disk_limit: 3400GiB
      scale: 4
      workers: 56
      credits_per_hour: "64"
    2xlarge:
      cpu_limit: 62
      memory_limit: 470GiB
      disk_limit: 3400GiB
      scale: 2
      workers: 56
      credits_per_hour: "32"
    xlarge:
      cpu_limit: 62
      memory_limit: 470GiB
      disk_limit: 3400GiB
      scale: 1
      workers: 56
      credits_per_hour: "16"
    large:
      cpu_limit: 30
      memory_limit: 235GiB
      disk_limit: 1645GiB
      scale: 1
      workers: 27
      credits_per_hour: "8"
    medium:
      cpu_limit: 14
      memory_limit: 112GiB
      disk_limit: 767GiB
      scale: 1
      workers: 12
      credits_per_hour: "4"
    small:
      cpu_limit: 6
      memory_limit: 48GiB
      disk_limit: 329GiB
      scale: 1
      workers: 5
      credits_per_hour: "2"
    xsmall:
      cpu_limit: 2
      memory_limit: 16GiB
      disk_limit: 109GiB
      scale: 1
      workers: 1
      credits_per_hour: "1"
    2xsmall:
      cpu_limit: 1
      memory_limit: 8GiB
      disk_limit: 54GiB
      scale: 1
      workers: 1
      credits_per_hour: "0.5"
    3xsmall:
      cpu_limit: 0.5
      memory_limit: 4GiB
      disk_limit: 27GiB
      scale: 1
      workers: 1
      credits_per_hour: "0.25"
    free:
      cpu_limit: 0.0
      credits_per_hour: "0"
      memory_limit: 0GiB
      disk_limit: 0GiB
      scale: 0
      workers: 0
      disabled: true
"""
print(json.dumps(yaml.safe_load(yml)))
