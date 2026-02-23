require_relative "test_helper"

class ObjectStoreTest < GitgresTest
  def test_write_and_read_blob
    content = "hello world"
    oid = write_object(3, content)

    result = @conn.exec_params(
      "SELECT type, size, content FROM git_object_read($1, decode($2, 'hex'))",
      [@repo_id, oid]
    )

    assert_equal 1, result.ntuples
    assert_equal "3", result[0]["type"]
    assert_equal content.bytesize.to_s, result[0]["size"]
    # content comes back as escaped bytea
    assert_equal content, @conn.unescape_bytea(result[0]["content"])
  end

  def test_write_returns_correct_oid
    content = "test content"
    oid = write_object(3, content)
    expected = git_hash_object(3, content)
    assert_equal expected, oid
  end

  def test_write_idempotent
    content = "duplicate test"
    oid1 = write_object(3, content)
    oid2 = write_object(3, content)
    assert_equal oid1, oid2

    # Should still be only one row
    result = @conn.exec_params(
      "SELECT count(*) FROM objects WHERE repo_id = $1 AND oid = decode($2, 'hex')",
      [@repo_id, oid1]
    )
    assert_equal "1", result[0]["count"]
  end

  def test_read_nonexistent
    result = @conn.exec_params(
      "SELECT * FROM git_object_read($1, decode('0000000000000000000000000000000000000000', 'hex'))",
      [@repo_id]
    )
    assert_equal 0, result.ntuples
  end

  def test_write_multiple_types
    blob_oid = write_object(3, "blob content")

    # Write a commit-like content (doesn't need to be valid git commit for storage)
    commit_content = "tree 0000000000000000000000000000000000000000\nauthor Test <test@test.com> 1234567890 +0000\ncommitter Test <test@test.com> 1234567890 +0000\n\ntest commit"
    commit_oid = write_object(1, commit_content)

    refute_equal blob_oid, commit_oid

    # Verify types are stored correctly
    result = @conn.exec_params(
      "SELECT type FROM git_object_read($1, decode($2, 'hex'))",
      [@repo_id, blob_oid]
    )
    assert_equal "3", result[0]["type"]

    result = @conn.exec_params(
      "SELECT type FROM git_object_read($1, decode($2, 'hex'))",
      [@repo_id, commit_oid]
    )
    assert_equal "1", result[0]["type"]
  end
end
