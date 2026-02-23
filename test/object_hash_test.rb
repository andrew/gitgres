require_relative "test_helper"

class ObjectHashTest < GitgresTest
  def test_blob_hash_matches_git
    content = "hello"
    expected = git_hash_object(3, content)

    result = @conn.exec_params(
      "SELECT encode(git_object_hash(3::smallint, $1::bytea), 'hex') AS hash",
      [{ value: content, format: 1 }]
    )
    assert_equal expected, result[0]["hash"]
  end

  def test_empty_blob_hash
    content = ""
    expected = git_hash_object(3, content)

    result = @conn.exec_params(
      "SELECT encode(git_object_hash(3::smallint, $1::bytea), 'hex') AS hash",
      [{ value: content, format: 1 }]
    )
    assert_equal expected, result[0]["hash"]
  end

  def test_binary_blob_hash
    content = (0..255).map(&:chr).join
    expected = git_hash_object(3, content)

    result = @conn.exec_params(
      "SELECT encode(git_object_hash(3::smallint, $1::bytea), 'hex') AS hash",
      [{ value: content, format: 1 }]
    )
    assert_equal expected, result[0]["hash"]
  end

  def test_type_name_function
    result = @conn.exec("SELECT git_type_name(1::smallint) AS name")
    assert_equal "commit", result[0]["name"]

    result = @conn.exec("SELECT git_type_name(2::smallint) AS name")
    assert_equal "tree", result[0]["name"]

    result = @conn.exec("SELECT git_type_name(3::smallint) AS name")
    assert_equal "blob", result[0]["name"]

    result = @conn.exec("SELECT git_type_name(4::smallint) AS name")
    assert_equal "tag", result[0]["name"]
  end

  def test_large_blob_hash
    content = "x" * 100_000
    expected = git_hash_object(3, content)

    result = @conn.exec_params(
      "SELECT encode(git_object_hash(3::smallint, $1::bytea), 'hex') AS hash",
      [{ value: content, format: 1 }]
    )
    assert_equal expected, result[0]["hash"]
  end
end
