package io.transwarp.batchinsert;

public class SampleCode {
    public void mian() {
        SQLOperation so = new SQLOperation();
        so.HyperbaseBatchInsertWithSql("");
        so.HyperbaseBatchInsertWithoutStructRowKey("tableName", "inputPath", null,false);
        so.HyperbaseBatchInsertWithStructRowKey("", "", null, null, true);
        so.Select("");
        so.close();
    }
}
