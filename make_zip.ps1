# Create a deploy/backup zip (Windows PowerShell)
# Run this from the parent directory that contains the 'ka-part_full_backup_20260205' folder.
Compress-Archive -Path ka-part_full_backup_20260205 -DestinationPath ka-part_full_backup_20260205.zip -Force
