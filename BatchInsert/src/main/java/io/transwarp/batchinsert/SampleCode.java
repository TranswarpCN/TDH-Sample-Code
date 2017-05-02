package io.transwarp.batchinsert;

public class SampleCode {
    // 主函数
    public static void main() {
        SQLOperation so = new SQLOperation();
        so.HyperbaseBatchInsertWithSql("");
        so.HyperbaseBatchInsertWithoutStructRowKey("tableName", "inputPath", null,false);
        so.HyperbaseBatchInsertWithStructRowKey("", "", null, null, true);
        so.Select("");
        so.close();
    }
}
