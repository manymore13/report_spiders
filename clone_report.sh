if [ -d "target" ]; then
    # 目录存在，删除它
    rm -r "target"
    echo "Directory deleted."
else
    echo "Directory does not exist."
fi
mkdir target
cd target
git clone https://github.com/manymore13/report.git
cd ..
mkdir gen_report
cp -r target/report/* gen_report