package com.ibeifeng.bigdata.offline.analystics.dimension.value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibeifeng.bigdata.offline.analystics.common.KpiType;

public class TimeOutputValue extends BaseStatsValueWritable {
    private String id; // id  --->u_ud 用户  u_mid 会员  u_sd 会话
    private long time; // 时间戳

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.id);
        out.writeLong(this.time);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.time = in.readLong();
    }

    @Override
    public KpiType getKpi() {
        // TODO Auto-generated method stub
        return null;
    }

}
