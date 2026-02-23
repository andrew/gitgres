require_relative "test_helper"

class TreeParseTest < GitgresTest
  def test_parse_single_entry_tree
    # Create a real tree using git
    dir = create_test_repo
    File.write(File.join(dir, "hello.txt"), "hello")
    system("git", "-C", dir, "add", "hello.txt", out: File::NULL, err: File::NULL)
    system("git", "-C", dir, "commit", "-m", "test", out: File::NULL, err: File::NULL)

    tree_oid = `git -C #{dir} rev-parse HEAD^{tree}`.strip
    tree_content = `git -C #{dir} cat-file tree #{tree_oid}`

    # Parse using our function
    result = @conn.exec_params(
      "SELECT mode, name, encode(entry_oid, 'hex') AS oid FROM git_tree_entries($1::bytea)",
      [{ value: tree_content, format: 1 }]
    )

    assert_equal 1, result.ntuples
    assert_equal "hello.txt", result[0]["name"]
    assert_equal "100644", result[0]["mode"]

    # Verify the blob OID matches
    expected_blob_oid = `git -C #{dir} rev-parse HEAD:hello.txt`.strip
    assert_equal expected_blob_oid, result[0]["oid"]

    FileUtils.rm_rf(dir)
  end

  def test_parse_multi_entry_tree
    dir = create_test_repo
    File.write(File.join(dir, "a.txt"), "aaa")
    File.write(File.join(dir, "b.txt"), "bbb")
    FileUtils.mkdir_p(File.join(dir, "subdir"))
    File.write(File.join(dir, "subdir", "c.txt"), "ccc")
    system("git", "-C", dir, "add", ".", out: File::NULL, err: File::NULL)
    system("git", "-C", dir, "commit", "-m", "test", out: File::NULL, err: File::NULL)

    tree_oid = `git -C #{dir} rev-parse HEAD^{tree}`.strip
    tree_content = `git -C #{dir} cat-file tree #{tree_oid}`

    result = @conn.exec_params(
      "SELECT mode, name, encode(entry_oid, 'hex') AS oid FROM git_tree_entries($1::bytea) ORDER BY name",
      [{ value: tree_content, format: 1 }]
    )

    assert_equal 3, result.ntuples
    names = result.map { |r| r["name"] }
    assert_includes names, "a.txt"
    assert_includes names, "b.txt"
    assert_includes names, "subdir"

    # subdir should be mode 40000
    subdir_row = result.find { |r| r["name"] == "subdir" }
    assert_equal "40000", subdir_row["mode"]

    FileUtils.rm_rf(dir)
  end

  def test_ls_tree_recursive
    dir = create_test_repo
    File.write(File.join(dir, "root.txt"), "root")
    FileUtils.mkdir_p(File.join(dir, "sub"))
    File.write(File.join(dir, "sub", "nested.txt"), "nested")
    system("git", "-C", dir, "add", ".", out: File::NULL, err: File::NULL)
    system("git", "-C", dir, "commit", "-m", "test", out: File::NULL, err: File::NULL)

    # Import the objects into our database
    tree_oid = `git -C #{dir} rev-parse HEAD^{tree}`.strip

    # Import all objects
    `git -C #{dir} rev-list --objects --all`.strip.split("\n").each do |line|
      oid = line.split(" ")[0]
      type_name = `git -C #{dir} cat-file -t #{oid}`.strip
      type_num = { "commit" => 1, "tree" => 2, "blob" => 3, "tag" => 4 }[type_name]
      content = `git -C #{dir} cat-file #{type_name} #{oid}`

      @conn.exec_params(
        "INSERT INTO objects (repo_id, oid, type, size, content) VALUES ($1, decode($2, 'hex'), $3, $4, $5) ON CONFLICT DO NOTHING",
        [@repo_id, oid, type_num, content.bytesize, { value: content, format: 1 }]
      )
    end

    # Test recursive tree walk
    result = @conn.exec_params(
      "SELECT mode, path, encode(oid, 'hex') AS oid, obj_type FROM git_ls_tree_r($1, decode($2, 'hex'))",
      [@repo_id, tree_oid]
    )

    paths = result.map { |r| r["path"] }
    assert_includes paths, "root.txt"
    assert_includes paths, "sub/"
    assert_includes paths, "sub/nested.txt"

    FileUtils.rm_rf(dir)
  end
end
