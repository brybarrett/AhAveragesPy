Bad implemtation / conversion of my 2020 auction averages (which was made into a sniper). Averaging system is extremely (4 years) outdated so dont trust. Purely for example / testing currently as of 03/25

### Currently the database includes over 7200 minutes (120 hours) of finished auctions (7200 api refreshes). Currently limited to about 1 request every 5 minutes due to githubs limit on how github actions chron scheduler work (1 request every 5 minutes)

### 1,127,999 unique BIN auctions that contain a buyer as of 11:40 03/06/2025 since *roughly* beginning of march 2025

## Database Storage Change (Snapshots)
Raw `*.db` files are no longer committed (they grew near / over GitHub's 100 MB limit). Instead compressed SQL dumps (`database.sql.gz`, `database2.sql.gz`) are stored. CI rebuilds the actual `.db` files for GitHub Pages deployment.

Rebuild locally:
```
gzip -dc database2.sql.gz | sqlite3 database2.db
```
This yields a faithful snapshot of the state at that commit.

[Database Viewer](https://ultimateboi.github.io/AhAveragesPy/)

# Repo Views
![Views](https://count.getloli.com/get/@UltimateBoi.AhAveragesPy?theme=3d-num)
