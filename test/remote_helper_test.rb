require_relative "test_helper"

class RemoteHelperTest < GitgresTest
  def setup
    super
    @helper = File.expand_path("../backend/git-remote-gitgres", __dir__)
    skip "git-remote-gitgres not built" unless File.executable?(@helper)

    # Repo name unique to this test run
    @remote_repo = "remote_test_#{$$}_#{rand(10000)}"

    # Initialize the remote repo in the database (outside the transaction
    # since the helper uses its own connection)
    @conn.exec("COMMIT")
    @conn.exec_params(
      "INSERT INTO repositories (name) VALUES ($1) ON CONFLICT DO NOTHING",
      [@remote_repo]
    )
  end

  def teardown
    # Clean up repos we created outside the transaction
    [@remote_repo, "test_repo"].each do |name|
      result = @conn.exec_params(
        "SELECT id FROM repositories WHERE name = $1", [name]
      )
      if result.ntuples > 0
        rid = result[0]["id"].to_i
        @conn.exec_params("DELETE FROM reflog WHERE repo_id = $1", [rid])
        @conn.exec_params("DELETE FROM refs WHERE repo_id = $1", [rid])
        @conn.exec_params("DELETE FROM objects WHERE repo_id = $1", [rid])
        @conn.exec_params("DELETE FROM repositories WHERE id = $1", [rid])
      end
    end
    @conn.close
  end

  def with_helper_on_path
    old_path = ENV["PATH"]
    ENV["PATH"] = "#{File.dirname(@helper)}:#{old_path}"
    yield
  ensure
    ENV["PATH"] = old_path
  end

  def test_push_and_clone_roundtrip
    source = create_test_repo
    File.write(File.join(source, "file.txt"), "hello\n")
    system("git", "-C", source, "add", ".", out: File::NULL, err: File::NULL)
    system("git", "-C", source, "commit", "-m", "init", out: File::NULL, err: File::NULL)

    with_helper_on_path do
      # Push via git
      system("git", "-C", source, "remote", "add", "pg",
        "gitgres::dbname=gitgres_test/#{@remote_repo}",
        out: File::NULL, err: File::NULL)
      result = system("git", "-C", source, "push", "pg", "main",
        out: File::NULL, err: File::NULL)
      assert result, "git push failed"

      # Clone via git
      clone_dir = Dir.mktmpdir("gitgres_clone")
      FileUtils.rm_rf(clone_dir)
      result = system("git", "clone",
        "gitgres::dbname=gitgres_test/#{@remote_repo}", clone_dir,
        out: File::NULL, err: File::NULL)
      assert result, "git clone failed"

      # Compare working trees
      source_files = Dir.glob("#{source}/**/*", File::FNM_DOTMATCH)
        .reject { |f| f.include?(".git") }
        .map { |f| [f.sub(source, ""), File.file?(f) ? File.read(f) : nil] }

      clone_files = Dir.glob("#{clone_dir}/**/*", File::FNM_DOTMATCH)
        .reject { |f| f.include?(".git") }
        .map { |f| [f.sub(clone_dir, ""), File.file?(f) ? File.read(f) : nil] }

      assert_equal source_files.sort, clone_files.sort

      # Verify git log matches
      source_log = `git -C #{source} log --oneline`.strip
      clone_log = `git -C #{clone_dir} log --oneline`.strip
      assert_equal source_log, clone_log

      FileUtils.rm_rf(clone_dir)
    end

    FileUtils.rm_rf(source)
  end

  def test_incremental_push_and_fetch
    source = create_test_repo
    File.write(File.join(source, "a.txt"), "first\n")
    system("git", "-C", source, "add", ".", out: File::NULL, err: File::NULL)
    system("git", "-C", source, "commit", "-m", "first", out: File::NULL, err: File::NULL)

    with_helper_on_path do
      system("git", "-C", source, "remote", "add", "pg",
        "gitgres::dbname=gitgres_test/#{@remote_repo}",
        out: File::NULL, err: File::NULL)
      system("git", "-C", source, "push", "pg", "main",
        out: File::NULL, err: File::NULL)

      # Clone
      clone_dir = Dir.mktmpdir("gitgres_clone")
      FileUtils.rm_rf(clone_dir)
      system("git", "clone",
        "gitgres::dbname=gitgres_test/#{@remote_repo}", clone_dir,
        out: File::NULL, err: File::NULL)

      # Add a second commit and push
      File.write(File.join(source, "b.txt"), "second\n")
      system("git", "-C", source, "add", ".", out: File::NULL, err: File::NULL)
      system("git", "-C", source, "commit", "-m", "second", out: File::NULL, err: File::NULL)
      system("git", "-C", source, "push", "pg", "main",
        out: File::NULL, err: File::NULL)

      # Fetch and pull in the clone
      result = system("git", "-C", clone_dir, "pull", "origin", "main",
        out: File::NULL, err: File::NULL)
      assert result, "git pull failed"

      # Verify the new file arrived
      assert File.exist?(File.join(clone_dir, "b.txt"))
      assert_equal "second\n", File.read(File.join(clone_dir, "b.txt"))

      clone_log = `git -C #{clone_dir} log --oneline`.strip.split("\n")
      assert_equal 2, clone_log.length

      FileUtils.rm_rf(clone_dir)
    end

    FileUtils.rm_rf(source)
  end
end
