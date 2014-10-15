KAVSyncTask
===========

SyncTask with KiiLib-Android-Volley

KiiLib-Android-Volleyと一緒に使うSyncTaskです。

まず、KAVSyncTaskを継承したクラスを作ります。ここではToDoSyncTaskとします。

```
public class ToDoSyncTask extends KAVSyncTask<ToDo> {
    //がんばってabstract methodを実装する
}
```

で、こんな感じに呼ぶと、ローカルのSQLiteのテーブルとKii Cloudの指定したバケツをそれっぽく同期してくれます。

```
ToDoSyncTask task = new ToDoSyncTask(mAPI, getActivity(), mDB);
task.sync(new SyncTask.SyncListener() {
  @Override
  public void onSuccess(SyncResult syncResult) {
    Log.v("Sync", "success upload=" + syncResult.getUploadCount() + " download=" + syncResult.getDownloadCount());
    getLoaderManager().restartLoader(ID_LOADER, null, mLoaderCallback);
  }

  @Override
  public void onError(SyncResult syncResult, Exception e) {
    Log.e("Sync", "failed exception=" + e, e);
  }
});
```
