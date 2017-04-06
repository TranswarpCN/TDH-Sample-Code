package io.transwarp.batchinsert;

/**
 * Created by 杨发林 on 2016/11/21.
 */
public class SampleCode {


    public void mian() {
        // TODO Auto-generated method stub
        SQLOperation so = new SQLOperation();
        so.HyperbaseBatchInsertWithSql("");
        so.HyperbaseBatchInsertWithoutStructRowKey("tableName", "inputPath", null,false);
        so.HyperbaseBatchInsertWithStructRowKey("", "", null, null, true);
        so.Select("");
        so.close();
    }


}
