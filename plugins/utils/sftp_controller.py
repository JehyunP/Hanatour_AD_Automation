import io
import paramiko
import logging

class SFTPController:
    def __init__(self, host: str, user: str, password: str, port: int = 22, timeout: int = 10):
        self.host = host
        self.user = user
        self.port = int(port)
        self.timeout = timeout

        try:
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh.connect(
                hostname=self.host,
                port=self.port,
                username=self.user,
                password=password,
                timeout=self.timeout,
            )
            logging.info("[SUCCESS] CONNECTED to SFTP")

        except paramiko.AuthenticationException as e:
            logging.error("[ERROR] SFTP AUTH FAILED - wrong username/password")
            raise

        except paramiko.SSHException as e:
            logging.error(f"[ERROR] SFTP SSH FAILED - {e}")
            raise

        except Exception as e:
            logging.error(f"[ERROR] SFTP CONNECT FAILED - {e}")
            raise

    
    def upload_df_tsv(self, df, remote_path, encoding = "utf-8-sig"):
        try:
            txt_buffer = io.StringIO()
            df.to_csv(txt_buffer, index=False, sep="\t", lineterminator="\r\n")

            data = txt_buffer.getvalue().encode(encoding)
            bio = io.BytesIO(data)
            bio.seek(0)

            sftp = self.ssh.open_sftp()
            try:
                sftp.putfo(bio, remote_path)
            finally:
                sftp.close()

            logging.info(f"[SUCCESS] Uploaded to SFTP: {remote_path}")

        except Exception as e:
            logging.error(f"[ERROR] SFTP upload failed: {remote_path}\n\t{e}")
            raise

    def close(self):
        try:
            if hasattr(self, "ssh"):
                self.ssh.close()
                logging.info("[INFO] SFTP connection closed")
        except Exception as e:
            logging.warning(f"[WARN] Failed to close SFTP connection cleanly: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False