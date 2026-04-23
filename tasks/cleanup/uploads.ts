import { getDatabase } from '~/lib/db'
import { env } from '~/lib/env'
import { getStorage } from '~/lib/storage'

const itemsPerPage = 10

export default defineTask({
  meta: {
    name: 'cleanup:uploads',
    description:
      'Delete uploads without activity for over 1 minute. Since parts are only a few megabytes each, we can be fairly aggressive in cleaning up abandoned uploads.',
  },
  async run() {
    if (env.DISABLE_CLEANUP_JOBS) return {}

    const oneMinuteAgo = Date.now() - 60 * 1000
    const db = await getDatabase()
    const storage = await getStorage()

    let deletedCount = 0
    let page = 0
    while (true) {
      const uploads = await db
        .selectFrom('uploads')
        .where(({ eb, or, and }) =>
          and([
            or([eb('lastPartUploadedAt', 'is', null), eb('lastPartUploadedAt', '<', oneMinuteAgo)]), // no parts uploaded or last part uploaded over 1 minute ago
            eb('createdAt', '<', oneMinuteAgo), // older than 1 minute
          ]),
        )
        .selectAll()
        .limit(itemsPerPage)
        .offset(page * itemsPerPage)
        .execute()

      for (const upload of uploads) {
        await db.transaction().execute(async (tx) => {
          // Re-check staleness inside the transaction. Between our SELECT and
          // now, completeUpload may have promoted this upload: it deletes the
          // row and hands `folderName` off to a storage_location. If we then
          // called deleteFolder we'd wipe the parts a fresh cache_entry is
          // pointing at, producing "points at missing storage" on the next GET.
          // Gating on the same staleness predicate means the DELETE affects 0
          // rows if anything has moved, and we bail before touching S3.
          const result = await tx
            .deleteFrom('uploads')
            .where('id', '=', upload.id)
            .where(({ eb, or, and }) =>
              and([
                or([
                  eb('lastPartUploadedAt', 'is', null),
                  eb('lastPartUploadedAt', '<', oneMinuteAgo),
                ]),
                eb('createdAt', '<', oneMinuteAgo),
              ]),
            )
            .executeTakeFirst()

          if (!result.numDeletedRows) return

          await storage.adapter.deleteFolder(upload.folderName)
          deletedCount++
        })
      }

      if (uploads.length < itemsPerPage) break
      page++
    }

    return {
      result: {
        deleted: deletedCount,
      },
    }
  },
})
