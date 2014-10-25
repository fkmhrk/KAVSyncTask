package jp.fkmsoft.libs.kabsynctask;

import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteOpenHelper;

import java.util.ArrayList;
import java.util.List;

import jp.fkmsoft.libs.kiilib.apis.BucketAPI;
import jp.fkmsoft.libs.kiilib.apis.ObjectAPI;
import jp.fkmsoft.libs.kiilib.apis.QueryResult;
import jp.fkmsoft.libs.kiilib.entities.KiiBucket;
import jp.fkmsoft.libs.kiilib.entities.KiiClause;
import jp.fkmsoft.libs.kiilib.entities.KiiObject;
import jp.fkmsoft.libs.kiilib.entities.QueryParams;
import jp.fkmsoft.libs.kiilib.volley.KiiVolleyAPI;
import jp.fkmsoft.libs.sync.SyncTask;

/**
 * Sync Task with KiiLib
 */
abstract public class KAVSyncTask<ENTITY> extends SyncTask<ENTITY> {
    private static final String FIELD_MODIFIED = "_modified";
    private static final String DEFAULT_PREF_NAME = "kavSync";

    private final KiiVolleyAPI mAPI;
    private final Context mContext;
    private final SQLiteOpenHelper mHelper;
    private SQLiteDatabase mDB;
    private long mCurrentTime;
    private long mLastLocalSyncTime;

    protected KAVSyncTask(KiiVolleyAPI api, Context context, SQLiteOpenHelper helper) {
        this.mAPI = api;
        this.mContext = context.getApplicationContext();
        this.mHelper = helper;
    }

    @Override
    protected void doFetch() {
        final KiiBucket bucket = getBucket();
        KiiClause clause = getFetchClause();
        if (clause != null) {
            clause = KiiClause.and(clause, KiiClause.greaterThan(FIELD_MODIFIED, getBiggestServerTime(), false));
        } else {
            clause = KiiClause.greaterThan(FIELD_MODIFIED, getBiggestServerTime(), false);
        }
        final QueryParams params = new QueryParams(clause);
        params.sortByAsc(FIELD_MODIFIED);

        mAPI.bucketAPI().query(bucket, params, new BucketAPI.QueryCallback<KiiBucket, KiiObject>() {
            private final List<ENTITY> mResults = new ArrayList<ENTITY>();

            @Override
            public void onSuccess(QueryResult<KiiBucket, KiiObject> kiiObjects) {
                for (KiiObject obj : kiiObjects) {
                    mResults.add(toEntity(obj));
                }
                if (kiiObjects.hasNext()) {
                    mAPI.bucketAPI().query(bucket, params, this);
                    return;
                }
                doneFetch(mResults);
            }

            @Override
            public void onError(Exception e) {
                failed(e);
            }
        });
    }

    @Override
    protected void prePutDownloadedObject() throws Exception {
        mDB = mHelper.getWritableDatabase();
        mDB.beginTransaction();

        mLastLocalSyncTime = getLastLocalSyncTime();
        mCurrentTime = System.currentTimeMillis();
    }

    @Override
    protected void putDownloadedObject(ENTITY entity) throws Exception {
        try {
            if (updateDownloadedEntity(entity, mLastLocalSyncTime)) {
                return;
            }
            if (insertDownloadedEntity(entity, mLastLocalSyncTime)) {
                return;
            }
            throw new SQLiteException();
        } catch (SQLiteException e) {
            mDB.endTransaction();
            throw e;
        }
    }

    @Override
    protected void postPutDownloadedObject() {
        mDB.setTransactionSuccessful();
        mDB.endTransaction();
    }

    @Override
    protected void doGetModifiedObjects() {
        String modifiedColumnName = getModifiedColumnName();
        String selection = modifiedColumnName + ">?";
        String[] args = { String.valueOf(mLastLocalSyncTime) };
        String orderBy = modifiedColumnName + " asc";

        Cursor cursor = mDB.query(getTableName(), null, selection, args, null, null, orderBy);
        if (cursor == null) {
            failed(new SQLiteException());
            return;
        }
        if (!cursor.moveToFirst()) {
            // empty result
            cursor.close();
            doneGetModifiedObjects(new ArrayList<ENTITY>());
            return;
        }

        List<ENTITY> result = toEntityList(cursor);
        cursor.close();
        doneGetModifiedObjects(result);
    }

    @Override
    protected void doUploadObject(ENTITY entity) {
        if (hasServerId(entity)) {
            updateEntityRequest(entity);
        } else {
            createEntityRequest(entity);
        }
    }

    @Override
    protected void putModifiedObject(ENTITY entity) {
        String[] args = {getLocalId(entity)};
        try {
            mDB.update(getTableName(), toContentValues(entity, mCurrentTime), getLocalIdWhereClause(), args);
            donePutModifiedObject(entity);
        } catch (SQLiteException e) {
            failedUpload(e);
        }

    }

    @Override
    protected void saveLastSyncTime() {
        SharedPreferences pref = mContext.getSharedPreferences(getPrefName(), Context.MODE_PRIVATE);
        SharedPreferences.Editor edit = pref.edit();
        edit.putLong(getPrefKey(), mCurrentTime);
        edit.apply();
    }

    // private methods

    /**
     * Gets the biggest modified time of local
     * @return The biggest modified time
     */
    protected long getLastLocalSyncTime() {
        SharedPreferences pref = mContext.getSharedPreferences(getPrefName(), Context.MODE_PRIVATE);
        return pref.getLong(getPrefKey(), 0);
    }

    protected String getPrefName() {
        return DEFAULT_PREF_NAME;
    }

    /**
     * Tries to update downloaded entity.
     * If you want to implement merge feature, override this method.
     * @param entity The entity to be updated.
     * @param lastLocalSyncTime The biggest modified time of local modification.
     * @return true if update is succeeded.
     * @throws SQLiteException is thrown when update operation is failed.
     */
    protected boolean updateDownloadedEntity(ENTITY entity, long lastLocalSyncTime) throws SQLiteException {
        String[] args = { getServerId(entity) };
        int count = mDB.update(getTableName(), toContentValues(entity, lastLocalSyncTime), getServerIdWhereClause(), args);
        return count == 1;
    }

    /**
     * Tries to insert downloaded entity
     * @param entity the entity to be inserted
     * @param lastLocalSyncTime The biggest modified time of local modification.
     * @return true if insertion is succeeded.
     * @throws SQLiteException is thrown when insert operation is failed.
     */
    private boolean insertDownloadedEntity(ENTITY entity, long lastLocalSyncTime) throws SQLiteException {
        long rowId = mDB.insert(getTableName(), null, toContentValues(entity, lastLocalSyncTime));
        return rowId != -1;
    }

    /**
     * Sends an update request to Kii Cloud
     * @param entity The entity to be updated.
     */
    private void updateEntityRequest(final ENTITY entity) {
        KiiBucket bucket = getBucket();
        KiiObject obj = toKiiObject(bucket, entity);
        mAPI.objectAPI().save(obj, new ObjectAPI.ObjectCallback<KiiBucket, KiiObject>() {
            @Override
            public void onSuccess(KiiObject kiiObject) {
                setServerIdAndModifiedTime(entity, kiiObject.getId(), kiiObject.getModifiedTime());
                doneUpload(entity);
            }

            @Override
            public void onError(Exception e) {
                failedUpload(e);
            }
        });
    }

    /**
     * Sends an create request to Kii Cloud
     * @param entity The entity to be created.
     */
    private void createEntityRequest(final ENTITY entity) {
        KiiBucket bucket = getBucket();
        KiiObject obj = toKiiObject(bucket, entity);
        mAPI.objectAPI().create(bucket, obj, new ObjectAPI.ObjectCallback<KiiBucket, KiiObject>() {
            @Override
            public void onSuccess(KiiObject kiiObject) {
                setServerIdAndModifiedTime(entity, kiiObject.getId(), kiiObject.getModifiedTime());
                doneUpload(entity);
            }

            @Override
            public void onError(Exception e) {
                failedUpload(e);
            }
        });
    }

    // abstract methods

    /**
     * Gets the bucket.
     * @return Bucket to be synchronized
     */
    protected abstract KiiBucket getBucket();

    /**
     * Gets the clause of fetching modified objects.
     * Library creates the actual clause with this result.
     * <pre>
     *     actual clause = (return of this method) AND (_modified > lastServerTime)
     * </pre>
     * If this method returns null, library uses the following clause.
     * <pre>
     *     actual clause = (_modified > lastServerTime)
     * </pre>
     * @return the clause
     */
    protected abstract KiiClause getFetchClause();

    /**
     * Gets the where clause for update. server id column must be string.
     * @return Where clause like "server_id=?". Only 1 '?' must be included.
     */
    protected abstract String getServerIdWhereClause();

    /**
     * Gets the where clause for update. local id column must be string.
     * @return Where clause like "_id=?". Only 1 '?' must be included.
     */
    protected abstract String getLocalIdWhereClause();

    /**
     * Gets the biggest modified time of server(max(_modified))
     * @return The biggest modified time
     */
    protected abstract long getBiggestServerTime();

    /**
     * Gets the key name for {@link android.content.SharedPreferences}
     * @return The key name.
     */
    protected abstract String getPrefKey();

    /**
     * Gets the name of table
     * @return The name of table
     */
    protected abstract String getTableName();

    /**
     * Gets the name of "modified" column. This method will be called when library will get modified objects.
     * @return The name of "modified" column.
     */
    protected abstract String getModifiedColumnName();

    /**
     * Gets the ID of this entity
     * @param entity The entity.
     * @return The ID of entity.
     */
    protected abstract String getServerId(ENTITY entity);

    /**
     * Gets the ID of local database
     * @param entity The entity.
     * @return The local ID of entity.
     */
    protected abstract String getLocalId(ENTITY entity);

    /**
     * Determines whether this entity has server ID.
     * @param entity The entity.
     * @return true if this entity has server ID.
     */
    protected abstract boolean hasServerId(ENTITY entity);

    /**
     * Converts entity to {@link android.content.ContentValues} for insert / update
     * @param entity The entity to be inserted or updated.
     * @param lastLocalSyncTime The modified time of local modification.
     * @return Converted ContentValues
     */
    protected abstract ContentValues toContentValues(ENTITY entity, long lastLocalSyncTime);

    /**
     * Converts cursor to ENTITY list. DO NOT close cursor after conversion.
     * @param cursor The cursor.
     * @return The list of ENTITY.
     */
    protected abstract List<ENTITY> toEntityList(Cursor cursor);

    /**
     * Converts {@link jp.fkmsoft.libs.kiilib.entities.KiiObject} to ENTITY
     * @param obj The fetched object
     * @return Converted ENTITY
     */
    protected abstract ENTITY toEntity(KiiObject obj);

    /**
     * Converts ENTITY to {@link jp.fkmsoft.libs.kiilib.entities.KiiObject}.
     * If entity has server ID, Converted KiiObject must have its ID.
     * @param bucket The bucket.
     * @param entity The entity.
     * @return Converted KiiObject
     */
    protected abstract KiiObject toKiiObject(KiiBucket bucket, ENTITY entity);

    /**
     * Sets server ID and modified time to entity.
     * @param entity The entity.
     * @param id The server ID.
     * @param modifiedTime The modified time of server object.
     */
    protected abstract void setServerIdAndModifiedTime(ENTITY entity, String id, long modifiedTime);
}
