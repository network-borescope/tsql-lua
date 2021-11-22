local _m = {}

_m.hosts = {
    ufes = {
        items = {
            options = { pop_id = {map_cat = { AC=1, AM=2, AP=3, BA=4 } }, interface_id=1, location=1 },
            selectors = { time=1 },
            t_measures = { counter=1, ['*']=1 },
            s_measures = { counter=1, ['*']=1, pkt_loss_norm_avg=1, rtt_ns_avg=1, download_kbps_avg=1, upload_kbps_avg=1 },
        }
    },
    rnp_ttls = {
        items = {
            options = { cliente=1, ttl=1, location=1 },
            selectors = { time=1 },
            t_measures = { counter=1, ['*']=1, quantidades=1 },
            s_measures = { counter=1, ['*']=1, hsum=1, hc=1 },
        }
    }
}

return _m
