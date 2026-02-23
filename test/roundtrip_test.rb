require_relative "test_helper"

class RoundtripTest < GitgresTest
  def test_import_and_verify_objects
    dir = create_test_repo
    File.write(File.join(dir, "hello.txt"), "hello world\n")
    system("git", "-C", dir, "add", ".", out: File::NULL, err: File::NULL)
    system("git", "-C", dir, "commit", "-m", "initial", out: File::NULL, err: File::NULL)

    File.write(File.join(dir, "goodbye.txt"), "goodbye\n")
    system("git", "-C", dir, "add", ".", out: File::NULL, err: File::NULL)
    system("git", "-C", dir, "commit", "-m", "second commit", out: File::NULL, err: File::NULL)

    # Count objects in the local repo
    local_objects = `git -C #{dir} rev-list --objects --all`.strip.split("\n")
    local_oids = local_objects.map { |l| l.split(" ")[0] }.uniq

    # Import each object
    local_oids.each do |oid|
      type_name = `git -C #{dir} cat-file -t #{oid}`.strip
      type_num = { "commit" => 1, "tree" => 2, "blob" => 3, "tag" => 4 }[type_name]
      content = `git -C #{dir} cat-file #{type_name} #{oid}`

      @conn.exec_params(
        "SELECT git_object_write($1, $2::smallint, $3::bytea)",
        [@repo_id, type_num, { value: content, format: 1 }]
      )
    end

    # Verify object count
    result = @conn.exec_params("SELECT count(*) FROM objects WHERE repo_id = $1", [@repo_id])
    assert_equal local_oids.length, result[0]["count"].to_i

    # Verify each object's hash matches
    local_oids.each do |oid|
      result = @conn.exec_params(
        "SELECT encode(oid, 'hex') AS db_oid FROM objects WHERE repo_id = $1 AND oid = decode($2, 'hex')",
        [@repo_id, oid]
      )
      assert_equal 1, result.ntuples, "Object #{oid} not found in database"
      assert_equal oid, result[0]["db_oid"]
    end

    FileUtils.rm_rf(dir)
  end

  def test_commit_view_after_import
    dir = create_test_repo
    File.write(File.join(dir, "test.txt"), "test\n")
    system("git", "-C", dir, "add", ".", out: File::NULL, err: File::NULL)

    env = { "GIT_AUTHOR_DATE" => "1234567890 +0000", "GIT_COMMITTER_DATE" => "1234567890 +0000" }
    system(env, "git", "-C", dir, "commit", "-m", "test commit", out: File::NULL, err: File::NULL)

    commit_oid = `git -C #{dir} rev-parse HEAD`.strip

    # Import objects
    `git -C #{dir} rev-list --objects --all`.strip.split("\n").each do |line|
      oid = line.split(" ")[0]
      type_name = `git -C #{dir} cat-file -t #{oid}`.strip
      type_num = { "commit" => 1, "tree" => 2, "blob" => 3, "tag" => 4 }[type_name]
      content = `git -C #{dir} cat-file #{type_name} #{oid}`

      @conn.exec_params(
        "SELECT git_object_write($1, $2::smallint, $3::bytea)",
        [@repo_id, type_num, { value: content, format: 1 }]
      )
    end

    # Test commit parsing via SQL
    result = @conn.exec_params(
      "SELECT c.author_name, c.message FROM objects o, LATERAL git_commit_parse(o.content) c WHERE o.repo_id = $1 AND o.type = 1",
      [@repo_id]
    )

    assert_equal 1, result.ntuples
    assert_equal "Test User", result[0]["author_name"]
    assert_match(/test commit/, result[0]["message"])

    FileUtils.rm_rf(dir)
  end
end
